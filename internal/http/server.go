package http

import (
	"github.com/go-kit/kit/log"
	"github.com/kavirajk/kv/internal/cluster"

	"encoding/json"
	"fmt"
	"net/http"
)

type Key string
type Value string

type Server struct {
	peer   *cluster.Peer
	logger log.Logger
}

func NewServer(peer *cluster.Peer, logger log.Logger) *Server {
	return &Server{
		peer:   peer,
		logger: logger,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method, path := r.Method, r.URL.Path
	switch {
	case method == "POST" && path == "/put":
		s.handlePUT(w, r)
	case method == "GET" && path == "/get":
		s.handleGET(w, r)
	default:
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePUT(w http.ResponseWriter, r *http.Request) {

	var putPayload PutPayload

	if err := json.NewDecoder(r.Body).Decode(&putPayload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	s.peer.SetState(putPayload.Key, putPayload.Value)

	b, err := json.Marshal(&cluster.Update{Key: putPayload.Key, Value: putPayload.Value})
	if err != nil {
		panic(err)
	}

	s.peer.QueueBroadcast(&cluster.Broadcast{Msg: append([]byte{byte(cluster.StateMsg)}, b...)})

	fmt.Fprintln(w, "OK")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGET(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "'key' param is missing", http.StatusBadRequest)
	}

	val := s.peer.GetState(cluster.Key(key))

	json.NewEncoder(w).Encode(GetResponse{Key: cluster.Key(key), Value: cluster.Value(val)})
	w.WriteHeader(http.StatusOK)
}

type PutPayload struct {
	Key   cluster.Key
	Value cluster.Value
}

type GetResponse struct {
	Key   cluster.Key
	Value cluster.Value
}
