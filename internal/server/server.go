package server

import (
	"net"
	"context"
	"fmt"
	"sync"
	
	"github.com/pkg/errors"
)

type Key string
type Value []byte

type Event struct {
	timestamp uint64
	fromPeer string
	data []byte
}

type SharedMap map[Key]Value

type Server struct {
	State SharedMap
	mu sync.Mutex
	addrStr string
	addr *net.UDPAddr

	// Peers network
	eventsSend chan <- Event
	eventsRcv <- chan Event
}


func New(addr string) (*Server, error) {
	uaddr , err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return nil, errors.Errorf("failed to resolve UDP addr: %s", addr)
	}
	
	return &Server{
		// State: make(map[[]byte][]byte),
		addr: uaddr,
		addrStr: addr,
	}, nil
}

func (s *Server) Loop(ctx context.Context) error {
	conn, err := net.ListenUDP("udp4", s.addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on UDPaddr: %s", s.addr)
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		_, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read from UDP connection: %s", raddr)
		}
		fmt.Printf("Received: %s\n", buf)
	}
}

func (s *Server) SendPeer(peerAddr string, msg string) error {
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

// func (s *Server) ServeHTTP(w http.ResponseWriter)
