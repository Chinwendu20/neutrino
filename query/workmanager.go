package query

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

const (
	// MinQueryTimeout is the timeout a query will be initially given. If
	// the peer given the query fails to respond within the timeout, it
	// will be given to the next peer with an increased timeout.
	MinQueryTimeout = 2 * time.Second

	// MaxQueryTimeout is the maximum timeout given to a single query.
	MaxQueryTimeout = 32 * time.Second
)

var (
	// ErrWorkManagerShuttingDown will be returned in case the WorkManager
	// is in the process of exiting.
	ErrWorkManagerShuttingDown = errors.New("WorkManager shutting down")
)

type batch struct {
	requests []*Request
	options  *queryOptions
}

// Worker is the interface that must be satisfied by workers managed by the
// WorkManager.
type Worker interface {
	// Run starts the worker. The worker will supply its peer with queries,
	// and handle responses from it. Results for any query handled by this
	// worker will be delivered on the results channel. quit can be closed
	// to immediately make the worker exit.
	//
	// The method is blocking, and should be started in a goroutine. It
	// will run until the peer disconnects or the worker is told to quit.
	Run(results chan<- *JobResult, quit <-chan struct{})

	// NewJob returns a channel where work that is to be handled by the
	// worker can be sent. If the worker reads a QueryJob from this
	// channel, it is guaranteed that a response will eventually be
	// delivered on the results channel (except when the quit channel has
	// been closed).
	NewJob() chan<- *QueryJob

	Peer() BlkHdrPeer

	RespChan() chan<- interface{}
}

// PeerRanking is an interface that must be satisfied by the underlying module
// that is used to determine which peers to prioritize querios on.
type PeerRanking interface {

	// Order sorst the slice of peers according to their ranking.
	Order(peers []BlkHdrPeer)
}

// activeWorker wraps a Worker that is currently running, together with the job
// we have given to it.
// TODO(halseth): support more than one active job at a time.
type activeWorker struct {
	w         Worker
	activeJob *QueryJob
	onExit    chan struct{}
}

// Config holds the configuration options for a new WorkManager.
type Config struct {
	// ConnectedPeers is a function that returns a channel where all
	// connected peers will be sent. It is assumed that all current peers
	// will be sent imemdiately, and new peers as they connect.
	//
	// The returned function closure is called to cancel the subscription.
	ConnectedPeers func() (<-chan Peer, func(), error)

	// NewWorker is function closure that should start a new worker. We
	// make this configurable to easily mock the worker used during tests.
	NewWorker func(BlkHdrPeer) Worker

	// Ranking is used to rank the connected peers when determining who to
	// give work to.
	Ranking PeerRanking
}

// WorkManager is the main access point for outside callers, and satisfies the
// QueryAccess API. It receives queries to pass to peers, and schedules them
// among available workers, orchestrating where to send them.
type WorkManager struct {
	cfg *Config

	// newBatches is a channel where new batches of queries will be sent to
	// the workDispatcher.
	newBatches chan *batch

	// jobResults is the common channel where results from queries from all
	// workers will be sent.
	jobResults chan *JobResult

	NewWorker func(Peer) Worker

	quit chan struct{}
	wg   sync.WaitGroup

	workersRWMutex sync.RWMutex
	workRWMtx      sync.Mutex

	workers map[string]*activeWorker
	work    *workQueue
}

// Compile time check to ensure WorkManager satisfies the Dispatcher interface.
var _ Dispatcher = (*WorkManager)(nil)

// New returns a new WorkManager with the regular worker implementation.
func New(cfg *Config) *WorkManager {
	return &WorkManager{
		cfg:        cfg,
		newBatches: make(chan *batch),
		jobResults: make(chan *JobResult, 2),
		quit:       make(chan struct{}),
		workers:    make(map[string]*activeWorker),
		work:       &workQueue{},
	}
}

// Start starts the WorkManager.
func (w *WorkManager) Start() error {
	// Init a work queue which will be used to sort the incoming queries in
	// a first come first served fashion. We use a heap structure such
	// that we can efficiently put failed queries back in the queue.

	heap.Init(w.work)

	w.wg.Add(2)
	go w.workDispatcher()
	go w.distributeBlkHeaderWork()

	return nil
}

// Stop stops the WorkManager and all underlying goroutines.
func (w *WorkManager) Stop() error {
	close(w.quit)
	w.wg.Wait()

	return nil
}

// workDispatcher receives batches of queries to be performed from external
// callers, and dispatches these to active workers.  It makes sure to
// prioritize the queries in the order they come in, such that early queries
// will be attempted completed first.
//
// NOTE: MUST be run as a goroutine.
func (w *WorkManager) workDispatcher() {
	defer w.wg.Done()

	// Get a peer subscription. We do it in this goroutine rather than
	// Start to avoid a deadlock when starting the WorkManager fetches the
	// peers from the server.
	peersConnected, cancel, err := w.cfg.ConnectedPeers()
	if err != nil {
		log.Errorf("Unable to get connected peers: %v", err)
		return
	}
	defer cancel()

	type batchProgress struct {
		timeout <-chan time.Time
		rem     int
		errChan chan error
	}

	// We set up a batch index counter to keep track of batches that still
	// have queries in flight. This lets us track when all queries for a
	// batch have been finished, and return an (non-)error to the caller.
	batchIndex := uint64(0)
	currentBatches := make(map[uint64]*batchProgress)

	// When the work dispatcher exits, we'll loop through the remaining
	// batches and send on their error channel.
	defer func() {
		for _, b := range currentBatches {
			b.errChan <- ErrWorkManagerShuttingDown
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := float64(0)
	currentQueries := make(map[float64]uint64)

Loop:
	for {
		// Otherwise the work queue is empty, or there are no workers
		// to distribute work to, so we'll just wait for a result of a
		// previous query to come back, a new peer to connect, or for a
		// new batch of queries to be scheduled.
		select {
		// Spin up a goroutine that runs a worker each time a peer
		// connects.
		case connectPeer := <-peersConnected:
			peer := connectPeer.(BlkHdrPeer)
			log.Debugf("Starting worker for peer %v",
				peer.Addr())

			r := w.cfg.NewWorker(peer)

			// We'll create a channel that will close after the
			// worker's Run method returns, to know when we can
			// remove it from our set of active workers.
			onExit := make(chan struct{})
			w.workersRWMutex.Lock()
			w.workers[peer.Addr()] = &activeWorker{
				w:         r,
				activeJob: nil,
				onExit:    onExit,
			}
			w.workersRWMutex.Unlock()

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer close(onExit)

				r.Run(w.jobResults, w.quit)
			}()

		// A new result came back.
		case result := <-w.jobResults:
			log.Tracef("Result for job %v received from peer %v "+
				"(err=%v)", result.Job.Index(),
				result.Peer.Addr(), result.Err)

			// Delete the job from the worker's active job, such
			// that the slot gets opened for more work.
			w.workersRWMutex.RLock()
			r := w.workers[result.Peer.Addr()]
			w.workersRWMutex.RUnlock()
			r.activeJob = nil

			// Get the index of this query's batch, and delete it
			// from the map of current queries, since we don't have
			// to track it anymore. We'll add it back if the result
			// turns out to be an error.
			batchNum, ok := currentQueries[result.Job.Index()]
			if !ok {
				continue
			}
			delete(currentQueries, result.Job.Index())
			batch := currentBatches[batchNum]

			switch {
			// If the query ended because it was canceled, drop it.
			case result.Err == ErrJobCanceled:
				log.Tracef("Query(%d) was canceled before "+
					"result was available from peer %v",
					result.Job.Index(), result.Peer.Addr())

				// If this is the first job in this batch that
				// was canceled, forward the error on the
				// batch's error channel.  We do this since a
				// cancellation applies to the whole batch.
				if batch != nil {
					batch.errChan <- result.Err
					delete(currentBatches, batchNum)

					log.Debugf("Canceled batch %v",
						batchNum)
					continue Loop
				}

			// If the query ended with any other error, put it back
			// into the work queue.
			case result.Err != nil:

				log.Warnf("Query(%d) from peer %v failed, "+
					"rescheduling: %v", result.Job.Index(),
					result.Peer.Addr(), result.Err)

				w.workRWMtx.Lock()
				heap.Push(w.work, result.Job)
				w.workRWMtx.Unlock()
				currentQueries[result.Job.Index()] = batchNum

			// Otherwise we got a successful result and  update the
			// status of the batch this query is a part of.
			default:

				if result.UnFinished {
					log.Debugf("Job %v is unfinished", result.Job.Index())
					log.Debugf("Length of testWork before push %v", w.work.Len())
					w.workRWMtx.Lock()
					heap.Push(w.work, &result.Job)
					batch.rem++
					log.Debugf("Length of testWork after push %v", w.work.Len())
					temp := w.work.Peek().(*QueryJob)
					w.workRWMtx.Unlock()
					log.Debugf("First element in the heap: %v", temp.Index())
					currentQueries[result.Job.Index()] = batchNum

					continue
				} else {
					log.Debugf("Job %v is Finished", result.Job.Index())
				}
				// Decrement the number of queries remaining in
				// the batch.
				if batch != nil {
					batch.rem--
					log.Debugf("Remaining jobs for batch "+
						"%v: %v ", batchNum, batch.rem)

					// If this was the last query in flight
					// for this batch, we can notify that
					// it finished, and delete it.
					if batch.rem == 0 {

						log.Debugf("Batch %v done",
							batchNum)
						return
					}
				}
			}

			// If the total timeout for this batch has passed,
			// return an error.
			if batch != nil {
				select {
				case <-batch.timeout:
					batch.errChan <- ErrQueryTimeout
					delete(currentBatches, batchNum)

					log.Warnf("Query(%d) failed with "+
						"error: %v. Timing out.",
						result.Job.Index(), result.Err)

					log.Debugf("Batch %v timed out",
						batchNum)

				default:
				}
			}

		// A new batch of queries where scheduled.

		// A new batch of queries where scheduled.
		case batch := <-w.newBatches:
			// Add all new queries in the batch to our work queue,
			// with priority given by the order they were
			// scheduled.
			log.Debugf("Adding new batch(%d) of %d queries to "+
				"work queue", batchIndex, len(batch.requests))

			for _, q := range batch.requests {
				w.workRWMtx.Lock()
				heap.Push(w.work, &QueryJob{
					JobIndex:   queryIndex,
					timeout:    MinQueryTimeout,
					encoding:   batch.options.encoding,
					cancelChan: batch.options.cancelChan,
					Request:    q,
				})
				w.workRWMtx.Unlock()
				currentQueries[queryIndex] = batchIndex
				queryIndex++
			}

			currentBatches[batchIndex] = &batchProgress{
				timeout: time.After(batch.options.timeout),
				rem:     len(batch.requests),
				errChan: batch.options.errChan,
			}
			batchIndex++
		case <-w.quit:
			return
		}
	}
}

// distributeBlkHeaderWork distributes getheader work gotten from the
// blkHeaderDispatcher

func (w *WorkManager) distributeWorkFunc(eligibleWorkerFunc func(*activeWorker, *QueryJob) bool) func(w *WorkManager) {

	return func(w *WorkManager) {
		defer w.wg.Done()

	Loop:
		for {

			// If the work queue is non-empty, we'll take out the first
			// element in order to distribute it to a worker.
			w.workersRWMutex.RLock()
			if w.work.Len() > 0 && len(w.workers) > 0 {
				w.workersRWMutex.RUnlock()

				w.workRWMtx.Lock()
				next := w.work.Peek().(*QueryJob)

				// Find the peers with free work slots available.
				var eligibleWorkers []BlkHdrPeer
				w.workersRWMutex.RLock()
				for _, r := range w.workers {
					w.workersRWMutex.RUnlock()

					if !eligibleWorkerFunc(r, next) {
						w.workersRWMutex.Lock()
						continue
					}

					log.Debugf("Num eligible worker: %v", len(eligibleWorkers))
					eligibleWorkers = append(eligibleWorkers, r.w.Peer())
					w.workersRWMutex.RLock()
				}
				w.workersRWMutex.RUnlock()

				// Use the historical data to rank them.
				w.cfg.Ranking.Order(eligibleWorkers)

				// Give the job to the highest ranked peer with free
				// slots available.
				//log.Debugf("Trying to give eligible worker work: Num eligible worker: %v", len(eligibleWorkers))
				for _, p := range eligibleWorkers {
					w.workersRWMutex.RLock()
					r := w.workers[p.Addr()]
					w.workersRWMutex.RUnlock()

					// The worker has free work slots, it should
					// pick up the query.
					log.Debugf("Giving Next job to peer: %v", r.w.Peer())
					select {
					case r.w.NewJob() <- next:
						log.Debugf("Sent job %v to worker %v",
							next.Index(), p)

						heap.Pop(w.work)
						w.workRWMtx.Unlock()

						r.activeJob = next

						// Go back to start of loop, to check
						// if there are more jobs to
						// distribute.
						continue Loop

					// Remove workers no longer active.
					case <-r.onExit:
						w.workersRWMutex.Lock()
						delete(w.workers, p.Addr())
						w.workersRWMutex.Unlock()
						continue

					case <-w.quit:
						w.workRWMtx.Unlock()
						return
					}

				}
				w.workRWMtx.Unlock()
			} else {
				w.workersRWMutex.RUnlock()
			}

		}
	}
}

func (w *WorkManager) isWorkerEligibleForBlkHdrFetch(r *activeWorker, next *QueryJob) bool {
	// Only one active job at a time is currently
	// supported.
	if r.activeJob != nil {
		return false
	}
	if !r.w.Peer().IsPeerBehindStartHeight(next.Request) {
		return false

	}

	return true

}

func (w *WorkManager) isWorkerEligibleForCFHdrFetch(r *activeWorker, _ *QueryJob) bool {
	// Only one active job at a time is currently
	// supported.
	if r.activeJob != nil {
		return false
	}

	return true

}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTO: Part of the Dispatcher interface.
func (w *WorkManager) Query(requests []*Request,
	options ...QueryOption) chan error {

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	errChan := make(chan error, 1)

	newBatch := &batch{
		requests: requests,
		options:  qo,
	}
	// Add query messages to the queue of batches to handle.
	select {
	case w.newBatches <- newBatch:
	case <-w.quit:
		errChan <- ErrWorkManagerShuttingDown
	}

	return newBatch.options.errChan
}

func (w *WorkManager) ResultChan() chan *JobResult {

	return w.jobResults

}
