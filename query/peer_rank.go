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

type expPeerRanking struct {
	rank  map[string]int64
	mutex sync.RWMutex
}

// A compile time check to ensure peerRanking satisfies the PeerRanking
// interface.
var _ PeerRanking = (*peerRanking)(nil)

var _ ExpPeerRanking = (*expPeerRanking)(nil)

// NewPeerRanking returns a new, empty ranking.
func NewPeerRanking() PeerRanking {
	return &peerRanking{
		rank: make(map[string]uint64),
	}
}

func NewExpPeerRanking() ExpPeerRanking {
	return &expPeerRanking{
		rank: make(map[string]int64),
	}
}

// Order sorts the given slice of peers based on their current score. If a
// peer has no current score given, the default will be used.
func (p *peerRanking) Order(peers []string) {
	sort.Slice(peers, func(i, j int) bool {
		p.mutex.RLock()
		score1, ok := p.rank[peers[i]]
		p.mutex.RUnlock()
		if !ok {
			score1 = defaultScore
		}
		p.mutex.RLock()
		score2, ok := p.rank[peers[j]]
		p.mutex.RUnlock()
		if !ok {
			score2 = defaultScore
		}
		return score1 < score2
	})
}

func (p *expPeerRanking) Order(peers []BlkHdrPeer) {
	sort.Slice(peers, func(i, j int) bool {

		return peers[i].LastReqDuration() < peers[j].LastReqDuration()
	})
}

// AddPeer adds a new peer to the ranking, starting out with the default score.
func (p *peerRanking) AddPeer(peer string) {

	p.mutex.RLock()
	if _, ok := p.rank[peer]; ok {

		p.mutex.RUnlock()
		return
	}

	p.mutex.RUnlock()
	p.mutex.Lock()
	p.rank[peer] = defaultScore

	p.mutex.Unlock()
}

// Punish increases the score of the given peer.
func (p *peerRanking) Punish(peer string) {
	p.mutex.RLock()
	score, ok := p.rank[peer]
	p.mutex.RUnlock()
	if !ok {
		return
	}

	// Cannot punish more.
	if score == worstScore {
		return
	}
	p.mutex.Lock()
	p.rank[peer] = score + 1
	p.mutex.Unlock()
}

// Reward decreases the score of the given peer.
// TODO(halseth): use actual response time when ranking peers.
func (p *peerRanking) Reward(peer string) {
	p.mutex.RLock()
	score, ok := p.rank[peer]
	p.mutex.RUnlock()
	if !ok {
		return
	}

	// Cannot reward more.
	if score == bestScore {
		return
	}
	p.mutex.Lock()
	p.rank[peer] = score - 1
	p.mutex.Unlock()
}
