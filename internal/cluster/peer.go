package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

type Key string
type Value string

// Peer represents a single node in the cluster.
type Peer struct {
	list  *memberlist.Memberlist
	state *State

	logger log.Logger
}

func NewPeer(logger log.Logger, bindAddr string, existingPeers []string) (*Peer, error) {

	state := NewState(log.With(logger, "component", "state"))

	cfg := memberlist.DefaultLocalConfig()

	host, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get hostname")
	}

	cfg.Name = fmt.Sprintf("%s:%s", host, uuid.New())

	host, portStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "bind address(%s) is not is right format", bindAddr)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, errors.Wrapf(err, "not a valid port(%s)", portStr)
	}

	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.Delegate = state
	cfg.Events = state

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cluster membership")
	}

	if _, err := list.Join(existingPeers); err != nil {
		return nil, errors.Wrap(err, "failed to join the cluster's existing member")
	}

	return &Peer{
		list:   list,
		state:  state,
		logger: logger,
	}, nil
}

func (p *Peer) Members() []*memberlist.Node {
	return p.list.Members()
}

func (p *Peer) GetState(key Key) Value {
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()

	return p.state.data[key]
}

func (p *Peer) SetState(key Key, val Value) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.data[key] = val
}

func (p *Peer) QueueBroadcast(broadcast *Broadcast) {
	p.state.broadcast.QueueBroadcast(broadcast)
}

type MsgType byte

const (
	StateMsg MsgType = 's'
)

// Update is an every type of update that can possible done on the
// shared state.
type Update struct {
	Key   Key
	Value Value
}

// Broadcast is how user spectific state data is addeded to
// broadcast queue. This is the actuall information that are communicated via
// peers by piggy backing the existing gossip.
type Broadcast struct {
	Msg []byte
}

func (b *Broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *Broadcast) Message() []byte {
	return b.Msg
}

func (b *Broadcast) Finished() {

}

// State implements memberlist.Delegate interface that enable way to
// share user data by piggybacking already existing gossip protocol
// by memberlist.
type State struct {
	data map[Key]Value
	mu   sync.RWMutex

	broadcast *memberlist.TransmitLimitedQueue
	logger    log.Logger
}

var (
	_ memberlist.Delegate      = &State{}
	_ memberlist.EventDelegate = &State{}
	_ memberlist.Broadcast     = &Broadcast{}
)

func NewState(logger log.Logger) *State {
	return &State{
		data: make(map[Key]Value),
		broadcast: &memberlist.TransmitLimitedQueue{
			NumNodes: func() int {
				// TODO(kavi): Fix it with dynamic memberlist.Members
				return 3
			},
			RetransmitMult: 3,
		},
		logger: logger,
	}
}

// Implements memberlist.Delegate
func (s *State) NodeMeta(limit int) []byte {
	return []byte{}
}

// Implements memberlist.Delegate
func (s *State) NotifyMsg(buf []byte) {
	if len(buf) == 0 {
		return
	}

	// Ignore other messages that are internal to memberlist itself.
	// We only care about SateMsg which are added to broadcast queue
	// by user application.
	if MsgType(buf[0]) == StateMsg {
		var update Update
		fmt.Println("Msg: ", string(buf[1:]))
		if err := json.Unmarshal(buf[1:], &update); err != nil {
			panic(err)
		}
		s.mu.Lock()
		s.data[update.Key] = update.Value
		s.mu.Unlock()
	}
}

// Implements memberlist.Delegate
func (s *State) GetBroadcasts(overhead, limit int) [][]byte {
	return s.broadcast.GetBroadcasts(overhead, limit)
}

// Implements memberlist.Delegate
func (s *State) LocalState(join bool) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := json.Marshal(s.data)
	if err != nil {
		panic(err)
	}
	return b
}

// Implements memberlist.Delegate
func (s *State) MergeRemoteState(buf []byte, join bool) {
	// fuck safety!

	if len(buf) == 0 {
		return
	}

	if !join {
		return
	}

	var state map[Key]Value

	if err := json.Unmarshal(buf, &state); err != nil {
		panic(err)
	}

	s.mu.Lock()
	for k, v := range state {
		s.data[k] = v
	}
	s.mu.Unlock()

}

// NotifyJoin implements memberlist.EventDelegate.
func (s *State) NotifyJoin(node *memberlist.Node) {
	s.logger.Log("node", node.Name, "event", "joined")
}

// NotifyJoin implements memberlist.EventDelegate.
func (s *State) NotifyLeave(node *memberlist.Node) {
	s.logger.Log("node", node.Name, "event", "left")
}

// NotifyJoin implements memberlist.EventDelegate.
func (s *State) NotifyUpdate(node *memberlist.Node) {
	s.logger.Log("node", node.Name, "event", "updated")
}
