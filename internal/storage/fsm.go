package storage

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/coreos/etcd/snap"
)

// FiniteStateMachine - simple key-value store backed by raft
type FiniteStateMachine struct {
	propose     chan<- string // write only channel for proposing updates
	rwMutex     sync.RWMutex
	kvStore     map[string]string // commited key-value pairs
	snapShotter *snap.Snapshotter // stores the state with snapshots
}

// TODO replace with proto
type kv struct {
	Key string
	Val string
}

// Lookup - return a value if found
func (fsm *FiniteStateMachine) Lookup(key string) (string, bool) {
	fsm.rwMutex.RLock()
	v, ok := fsm.kvStore[key]
	fsm.rwMutex.RUnlock()
	return v, ok
}

// Propose - send a value to the propose channel
func (fsm *FiniteStateMachine) Propose(key string, value string) {
	var buf bytes.Buffer
	// TODO replace with proto
	if err := gob.NewEncoder(&buf).Encode(kv{key, value}); err != nil {
		log.Fatal(err)
	}
	fsm.propose <- buf.String()
}

func (fsm *FiniteStateMachine) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := fsm.snapShotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := fsm.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		fsm.rwMutex.Lock()
		fsm.kvStore[dataKv.Key] = dataKv.Val
		fsm.rwMutex.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

// GetKVStore return the json bytes of the internal map
func (fsm *FiniteStateMachine) GetKVStore() ([]byte, error) {
	fsm.rwMutex.Lock()
	defer fsm.rwMutex.Unlock()
	return json.Marshal(fsm.kvStore)
}

// load snapshot into map
func (fsm *FiniteStateMachine) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	fsm.rwMutex.Lock()
	fsm.kvStore = store
	fsm.rwMutex.Unlock()
	return nil
}
