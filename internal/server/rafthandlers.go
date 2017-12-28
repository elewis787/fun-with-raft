package server

import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"gitlab.com/ethanlewis787/fun-with-raft/internal/storage"
)

// RaftFSMServer - object that implements server for GET/PUT/POST/DELETE fsm operations
type raftFSMServer struct {
	fsm          *storage.FiniteStateMachine
	configChange chan<- raftpb.ConfChange // write only channel for updating configs
}

// NewRaftFSMServer - return an initialized raft fsm server
func NewRaftFSMServer(fsm *storage.FiniteStateMachine, port string, configChange chan<- raftpb.ConfChange) *http.Server {
	return &http.Server{
		Addr: ":" + port,
		Handler: &raftFSMServer{
			fsm:          fsm,
			configChange: configChange,
		},
	}
}

func (rf *raftFSMServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI

	defer r.Body.Close()

	switch {
	// Add k,v
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		rf.fsm.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
		return
	// Lookup k,v
	case r.Method == "GET":
		if v, ok := rf.fsm.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
		return
	// Post Config change ( may or maynot be updated we don't wait)
	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
			Context: url,
		}

		rf.configChange <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
		return
	// Delete k,v
	case r.Method == "DELETE":
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeID,
		}
		rf.configChange <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
		return

	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

}
