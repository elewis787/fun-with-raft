package raftconsensus

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

var snapshotCatchUpEntriesN uint64 = 10000
var defaultSnapCount uint64 = 10000

// RaftNode - wrapper for raft consensus alg.
type RaftNode struct {
	propose      <-chan string            // read only channel of proposed messages (k,v)
	configChange <-chan raftpb.ConfChange // proposed cluster config changes
	commits      chan<- *string           // entries commited to log (k,v)
	errors       chan<- error             // errors from raft session

	id           int      // client ID for raft session
	peers        []string // raft peer urls
	join         bool     // node is joining an existing cluster
	walDir       string   // path to the write ahead log directory
	snapshotFunc func() ([]byte, error)
	lastIndex    uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node          raft.Node
	storage       *raft.MemoryStorage
	writeAheadLog *wal.WAL

	snapshotter      *snap.Snapshotter
	snapShotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

// Process -
func (rn *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}

// IsIDRemoved -
func (rn *RaftNode) IsIDRemoved(id uint64) bool { return false }

// ReportUnreachable -
func (rn *RaftNode) ReportUnreachable(id uint64) {}

// ReportSnapshot -
func (rn *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// saveSnap - save a snapshot
func (rn *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	// musrt save the snapshot index to the Write Ahead Log before saving the
	// snapshot to maitain the invariant that we only open the wal at
	// previously-saved snaphost index.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// save snap shot to WAL
	if err := rn.writeAheadLog.SaveSnapshot(walSnap); err != nil {
		return err
	}

	// save snap to snapshotter
	if err := rn.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	// release the WAL lock
	if err := rn.writeAheadLog.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}

	return nil
}

// entriesToApply - return a list of new entries to add to the log
func (rn *RaftNode) entriesToApply(entries []raftpb.Entry) []raftpb.Entry {
	var newEntries []raftpb.Entry

	if len(entries) == 0 {
		return newEntries
	}

	firstIdx := entries[0].Index
	if firstIdx > rn.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rn.appliedIndex)

	}

	if rn.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		newEntries = entries[rn.appliedIndex-firstIdx+1:]
	}

	return newEntries
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rn *RaftNode) publishEntries(entries []raftpb.Entry) bool {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(entry.Data)
			select {
			case rn.commits <- &s:
			case <-rn.stopc:
				return false
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
		// after commit, update the applied index
		rn.appliedIndex = entry.Index
		if entry.Index == rn.lastIndex {
			select {
			case rn.commits <- nil:
			case <-rn.stopc:
				return false
			}
		}
	}
	return true
}

func (rn *RaftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rn *RaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.walDir) {
		if err := os.Mkdir(rn.walDir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rn.walDir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rn.walDir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}
	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rn *RaftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rn.id)
	snapshot := rn.loadSnapshot()
	w := rn.openWAL(snapshot)
	_, st, entries, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rn.storage = raft.NewMemoryStorage()
	if snapshot != nil {
		rn.storage.ApplySnapshot(*snapshot)
	}
	rn.storage.SetHardState(st)
	// append to storage so raft starts at the right place in log
	rn.storage.Append(entries)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(entries) > 0 {
		rn.lastIndex = entries[len(entries)-1].Index
	} else {
		rn.commits <- nil
	}
	return w
}

func (rn *RaftNode) stopHTTP() {
	rn.transport.Stop()
	close(rn.httpstopc)
	<-rn.httpdonec
}

// stop closes http, closes all channels, and stops raft.
func (rn *RaftNode) stop() {
	rn.stopHTTP()
	close(rn.commits)
	close(rn.errors)
	rn.node.Stop()
}

func (rn *RaftNode) writeError(err error) {
	rn.stopHTTP()
	close(rn.commits)
	rn.errors <- err
	close(rn.errors)
	rn.node.Stop()
}

func (rn *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rn.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rn.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rn.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rn.appliedIndex)
	}

	rn.commits <- nil // trigger kvstore to load snapshot

	rn.confState = snapshotToSave.Metadata.ConfState
	rn.snapshotIndex = snapshotToSave.Metadata.Index
	rn.appliedIndex = snapshotToSave.Metadata.Index
}

func (rn *RaftNode) maybeTriggerSnapshot() {
	if rn.appliedIndex-rn.snapshotIndex <= rn.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rn.appliedIndex, rn.snapshotIndex)
	data, err := rn.snapshotFunc()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rn.storage.CreateSnapshot(rn.appliedIndex, &rn.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rn.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rn.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rn.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rn.storage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rn.snapshotIndex = rn.appliedIndex
}

func (rn *RaftNode) serveChannels() {
	snap, err := rn.storage.Snapshot()
	if err != nil {
		panic(err)
	}
	rn.confState = snap.Metadata.ConfState
	rn.snapshotIndex = snap.Metadata.Index
	rn.appliedIndex = snap.Metadata.Index
	defer rn.writeAheadLog.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64

		for rn.propose != nil && rn.configChange != nil {
			select {
			case prop, ok := <-rn.propose:
				if !ok {
					rn.propose = nil
				} else {
					// blocks until accepted by raft state machine
					rn.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rn.configChange:
				if !ok {
					rn.configChange = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rn.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rn.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rn.node.Ready():
			rn.writeAheadLog.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.saveSnap(rd.Snapshot)
				rn.storage.ApplySnapshot(rd.Snapshot)
				rn.publishSnapshot(rd.Snapshot)
			}
			rn.storage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)
			if ok := rn.publishEntries(rn.entriesToApply(rd.CommittedEntries)); !ok {
				rn.stop()
				return
			}
			rn.maybeTriggerSnapshot()
			rn.node.Advance()

		case err := <-rn.transport.ErrorC:
			rn.writeError(err)
			return

		case <-rn.stopc:
			rn.stop()
			return
		}
	}

}

func (rn *RaftNode) serveRaft() {
	url, err := url.Parse(rn.peers[rn.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rn.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rn.transport.Handler()}).Serve(ln)
	select {
	case <-rn.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rn.httpdonec)
}

// -- Helper -- //

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
