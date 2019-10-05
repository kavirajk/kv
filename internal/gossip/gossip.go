package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kavirajk/kv/internal/config"
	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Type int

const (
	GossipPush Type = iota
	GossipPull
)

type Key string
type Value string

type UpdateEvent struct {
	Timestamp uint64
	Key       Key
	Val       Value
}

type SharedMap map[Key]Value

type Node struct {
	listenAddr  string
	gossipCycle time.Duration
	gossipType  Type

	state SharedMap
	smu   sync.Mutex

	peers []config.Peer
	pmu   sync.Mutex

	updateEvents []UpdateEvent
	umu          sync.Mutex
}

func New(addr string, gossipCycle time.Duration, peers []config.Peer) *Node {
	return &Node{
		listenAddr:   addr,
		gossipCycle:  gossipCycle,
		peers:        peers,
		updateEvents: make([]UpdateEvent, 0),
		state:        make(SharedMap),
		gossipType:   GossipPush,
	}
}

func (g *Node) Loop(ctx context.Context) {
	// TODO: use ctx and return error
	for {
		time.Sleep(g.gossipCycle)
		if len(g.peers) <= 0 {
			continue
		}
		peer := g.selectPeer()
		switch g.gossipType {
		case GossipPush:
			if err := g.sendUpdateEventsToPeer(peer); err != nil {
				panic(err)
			}
		case GossipPull:
			g.sendUpdatePullRequestToPeer(peer)
		default:
			fmt.Println("Invalid gossip type. Skipping")
		}
		fmt.Println("Peers: ", g.peers)
	}
}

func (nn *Node) ListenLoop(ctx context.Context) error {
	// TODO: use ctx and return err
	addr, err := net.ResolveUDPAddr("udp4", nn.listenAddr)
	if err != nil {
		return errors.Wrapf(err, "NOT A VALID listenADDR: %s", nn.listenAddr)
	}

	for {
		conn, err := net.ListenUDP("udp4", addr)
		if err != nil {
			return errors.Wrapf(err, "failed to listen on UDPaddr: %s", addr)
		}

		buf := make([]byte, 1024)

		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return errors.Wrapf(err, "failed to read from UDP connection: %s", raddr)
		}

		fmt.Printf("Received: %s\n", buf[:n], nn, conn, raddr)

		nn.addPeer(config.Peer{Addr: raddr.String()})

		updates := []UpdateEvent{}

		if err := json.Unmarshal(buf[:n], &updates); err != nil {
			fmt.Println("Wrong payload", err)
		}

		nn.merge(updates)
		conn.Close()
	}
}

func (s *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (s *Node) handlePUT(w http.ResponseWriter, r *http.Request) {

	var putPayload PutPayload

	if err := json.NewDecoder(r.Body).Decode(&putPayload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	fmt.Println("Put payload received:", putPayload)

	s.smu.Lock()
	s.state[putPayload.Key] = putPayload.Val
	s.smu.Unlock()

	fmt.Println("updated the state")

	s.umu.Lock()
	s.updateEvents = append(s.updateEvents, UpdateEvent{Timestamp: uint64(time.Now().Unix()), Key: putPayload.Key, Val: putPayload.Val})
	s.umu.Unlock()

	fmt.Println("updated the updated events state")

	fmt.Fprintln(w, "OK")
	w.WriteHeader(http.StatusOK)
}

func (s *Node) handleGET(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "'key' param is missing", http.StatusBadRequest)
	}

	s.smu.Lock()
	defer s.smu.Unlock()

	val := s.state[Key(key)]

	json.NewEncoder(w).Encode(GetResponse{Key: Key(key), Val: val})
	w.WriteHeader(http.StatusOK)
}

type PutPayload struct {
	Key Key
	Val Value
}

type GetResponse struct {
	Key Key
	Val Value
}

func (n *Node) merge(updates []UpdateEvent) {
	// Fuck safetyness! :P
	n.smu.Lock()
	defer n.smu.Unlock()

	for _, update := range updates {
		n.state[update.Key] = update.Val
	}
}

func (n *Node) sendUpdatePullRequestToPeer(peer config.Peer) {
	panic("TODO")
}

func (n *Node) sendUpdateEventsToPeer(peer config.Peer) error {
	n.umu.Lock()
	defer n.umu.Unlock()

	addr, err := net.ResolveUDPAddr("udp4", peer.Addr)
	if err != nil {
		return errors.Wrapf(err, "Wrong peer address: %s", peer.Addr)
	}

	myAddr, err := net.ResolveUDPAddr("udp4", n.listenAddr)
	if err != nil {
		return errors.Wrapf(err, "Wrong my address: %s", myAddr)
	}

	fmt.Println("My ip: ", myAddr)

	conn, err := net.DialUDP("udp4", myAddr, addr)
	if err != nil {
		return errors.Wrapf(err, "failed to dial to peer addr: %s", addr)
	}

	if err := json.NewEncoder(conn).Encode(n.updateEvents); err != nil {
		return errors.Wrapf(err, "failed to encode update")
	}
	n.updateEvents = []UpdateEvent{}

	return nil
}

// addPeer adds given peer into the node only if it not exists
// already.
func (g *Node) addPeer(peer config.Peer) {
	g.pmu.Lock()
	defer g.pmu.Unlock()

	found := false

	for _, p := range g.peers {
		if p.Addr == peer.Addr {
			found = true
			break
		}
	}

	if !found {
		g.peers = append(g.peers, peer)
	}
}

// selectPeer randomly choose peer among all the available peers of the
// this given node.
func (g *Node) selectPeer() config.Peer {
	return g.peers[rand.Intn(len(g.peers))]
}
