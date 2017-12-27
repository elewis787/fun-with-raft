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
	v, ok := s.kvStore[key]
	fsm.rwMutex.RUnlock()
	return v, ok
}

// Propose - send a value to the propose channel
func (fsm *FiniteStateMachine) Propose(key string, value string) {
	var buf bytes.Buffer
	// TODO replace with proto
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	fsm.propose <- buf.String()
}

// return the json bytes of the internal map
func (fsm *FiniteStateMachine) getKVStore() ([]byte, error) {
	fsm.rwMutex.Lock()
	defer fsm.rwMutex.Unlock()
	return json.Marshal(s.kvStore)
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
