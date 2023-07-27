package query

import (
	"sort"
	"sync"
)

const (
	// bestScore is the best score a peer can get after multiple rewards.
	bestScore = 0

	// defaultScore is the score given to a peer when it hasn't been
	// rewarded or punished.
	defaultScore = 4

	// worstScore is the worst score a peer can get after multiple
	// punishments.
	worstScore = 8
)

// peerRanking is a struct that keeps history of peer's previous query success
// rate, and uses that to prioritise which peers to give the next queries to.
type peerRanking struct {
	// rank keeps track of the current set of peers and their score. A
	// lower score is better.
	rank  map[string]uint64
	mutex sync.RWMutex
}

// A compile time check to ensure peerRanking satisfies the PeerRanking
// interface.
var _ PeerRanking = (*peerRanking)(nil)

// NewPeerRanking returns a new, empty ranking.
func NewPeerRanking() PeerRanking {
	return &peerRanking{
		rank: make(map[string]uint64),
	}
}

// Order sorts the given slice of peers based on their current score. If a
// peer has no current score given, the default will be used.
func (p *peerRanking) Order(peers []BlkHdrPeer) {
	sort.Slice(peers, func(i, j int) bool {

		return peers[i].LastReqDuration() < peers[j].LastReqDuration()
	})
}
