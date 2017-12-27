package raftconsensus

import (
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
)

// RaftNode - wrapper for raft consensus alg.
type RaftNode struct {
	propose      <-chan string            // read only channel of proposed messages (k,v)
	configChange <-chan raftpb.ConfChange // proposed cluster config changes
	commit       chan<- *string           // entries commited to log (k,v)
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

var defaultSnapCount uint64 = 10000
