package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kavirajk/kv/internal/config"
)

type Key string
type Value string

type Event struct {
	timestamp uint64
	data      []byte
}

type SharedMap map[Key]Value

func (s SharedMap) Serialize() ([]byte, error) {
	panic("TODO!")
	return nil, nil
}

func (s SharedMap) Merge(t SharedMap) error {
	return nil
}

type Server struct {
	state   SharedMap
	mu      sync.Mutex
	addrStr string
	addr    *net.UDPAddr
	peers   []config.Peer

	// Peers network
	events chan Event
}

func New(addr string, peers []config.Peer) (*Server, error) {
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return nil, errors.Errorf("failed to resolve UDP addr: %s", addr)
	}

	return &Server{
		state:   make(map[Key]Value),
		addr:    uaddr,
		addrStr: addr,
		events:  make(chan Event, 1),
		peers:   peers,
	}, nil
}

func (s *Server) EventsRcvLoop(ctx context.Context) error {
	conn, err := net.ListenUDP("udp4", s.addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on UDPaddr: %s", s.addr)
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read from UDP connection: %s", raddr)
		}
		fmt.Printf("Received: %s\n", buf[:n])
		// Ignore buf boundary error
		// TODO(kavi)
		payload := PutPayload{}
		if err := json.Unmarshal(buf[:n], &payload); err != nil {
			fmt.Println("Wrong payload", err)
		}

		s.mu.Lock()
		s.state[Key(payload.Key)] = Value(payload.Value)
		s.mu.Unlock()
	}
}

func (s *Server) EventsSendLoop(ctx context.Context) error {
	for {
		select {
		case event := <-s.events:
			fmt.Println("Even here??", event, s.peers)
			for _, peer := range s.peers {
				if err := s.SendPeer(peer.Addr, event.data); err != nil {
					panic(err)
				}
			}
		case <-ctx.Done():
			fmt.Println("context canceled", ctx.Err())
		}
	}
}

func (s *Server) SendPeer(peerAddr string, msg []byte) error {
	fmt.Println("sending to peer: ", peerAddr, "msg: ", msg)
	addr, err := net.ResolveUDPAddr("udp4", peerAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to resolve peer addr: %s", peerAddr)
	}
	if peerAddr == s.addrStr {
		// its me skip it
		// TODO(kavi): Fix it
		fmt.Println("Skipping!!!!")
		return nil
	}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return errors.Wrapf(err, "failed to dial peer addr: %s", addr)
	}
	_, err = conn.Write([]byte(msg))
	if err != nil {
		return errors.Wrap(err, "failed to write to UDP connection")
	}
	return nil
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

	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[Key(putPayload.Key)] = Value(putPayload.Value)

	go func(key Key, val Value) {
		b, err := json.Marshal(PutPayload{Key: string(key), Value: string(val)})
		if err != nil {
			fmt.Println("wrong payload while sending event")
			return
		}
		fmt.Println("sending event")
		s.events <- Event{timestamp: uint64(time.Now().Unix()), data: b}
		fmt.Println("event sent")

	}(Key(putPayload.Key), Value(putPayload.Value))

	fmt.Fprintln(w, "OK")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGET(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "'key' param is missing", http.StatusBadRequest)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	val := s.state[Key(key)]

	json.NewEncoder(w).Encode(GetResponse{Key: key, Value: string(val)})
	w.WriteHeader(http.StatusOK)
}

type PutPayload struct {
	Key   string
	Value string
}

type GetResponse struct {
	Key   string
	Value string
}
