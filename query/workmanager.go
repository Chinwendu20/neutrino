package query

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
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
	Run(results chan<- *jobResult, quit <-chan struct{})

	// NewJob returns a channel where work that is to be handled by the
	// worker can be sent. If the worker reads a QueryJob from this
	// channel, it is guaranteed that a response will eventually be
	// delivered on the results channel (except when the quit channel has
	// been closed).
	NewJob() chan<- *QueryJob

	Peer() BlkHdrPeer
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
	ConnectedPeers func() (<-chan BlkHdrPeer, func(), error)

	// NewWorker is function closure that should start a new worker. We
	// make this configurable to easily mock the worker used during tests.
	NewWorker func(BlkHdrPeer, string) Worker

	// Order is used to rank the connected peers when determining who to
	// give work to.
	Order func(peers []BlkHdrPeer)

	// This function is used to determine peers that are eligible to be sent a job
	// while distributing tasks in the work mananger
	IsEligibleWorkerFunc func(r *activeWorker, next *QueryJob) bool
	Temp                 string
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
	jobResults chan *jobResult

	NewWorker func(Peer) Worker

	quit chan struct{}
	wg   sync.WaitGroup

	workersRWMutex sync.RWMutex
	workRWMtx      sync.RWMutex

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
		jobResults: make(chan *jobResult, 1),
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

	log.Debugf("starting wormanager for %s", w.cfg.Temp)
	heap.Init(w.work)

	w.wg.Add(2)
	go w.workDispatcher()
	go w.distributeWorkFunc(w.cfg.IsEligibleWorkerFunc)()

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
	temp := w.cfg.Temp
	log.Debugf("Inside work dispatcher for %s", temp)
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
		timeout   <-chan time.Time
		rem       int
		errChan   chan error
		noTimeout bool
	}

	// We set up a batch index counter to keep track of batches that still
	// have queries in flight. This lets us track when all queries for a
	// batch have been finished, and return an (non-)error to the caller.
	batchIndex := uint64(0)
	currentBatches := make(map[uint64]*batchProgress)

	// When the work dispatcher exits, we'll loop through the remaining
	// batches and send on their error channel.
	defer func() {
		log.Debugf("%v Out of work dispatcher", temp)
		for _, b := range currentBatches {
			b.errChan <- ErrWorkManagerShuttingDown
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := float64(0)
	currentQueries := make(map[float64]uint64)

	for {
		// Otherwise the work queue is empty, or there are no workers
		// to distribute work to, so we'll just wait for a result of a
		// previous query to come back, a new peer to connect, or for a
		// new batch of queries to be scheduled.
		log.Debugf("%v Waiting for ", temp)
		select {
		// Spin up a goroutine that runs a worker each time a peer
		// connects.
		case connectPeer := <-peersConnected:
			peer := connectPeer.(BlkHdrPeer)
			w.workersRWMutex.RLock()
			_, ok := w.workers[peer.Addr()]
			w.workersRWMutex.RUnlock()
			if ok {
				log.Debugf("%v Worker with peer address already exists", temp)
				continue
			}
			log.Debugf("(%) Starting worker for peer %v", temp,
				peer.Addr())

			r := w.cfg.NewWorker(peer, temp)

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
			fmt.Printf("Added worker (%v)\n", peer.Addr())

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer close(onExit)

				r.Run(w.jobResults, w.quit)
			}()

		// A new result came back.
		case result := <-w.jobResults:
			log.Debugf("%v Result for job %v received from peer %v "+
				"(err=%v)", temp, result.job.Index(),
				result.peer.Addr(), result.err)

			// Delete the job from the worker's active job, such
			// that the slot gets opened for more work.
			w.workersRWMutex.RLock()
			r := w.workers[result.peer.Addr()]
			w.workersRWMutex.RUnlock()
			if result.err != ErrQueryTimeout {
				log.Debugf("%vResult for job %v received no timeout err for workmanager %v "+
					"(err=%v)", temp, result.job.Index(),
					result.peer.Addr(), result.err)
				r.activeJob = nil
			}
			// Get the index of this query's batch, and delete it
			// from the map of current queries, since we don't have
			// to track it anymore. We'll add it back if the result
			// turns out to be an error.
			batchNum, ok := currentQueries[result.job.Index()]
			if !ok {
				continue
			}
			delete(currentQueries, result.job.Index())
			batch := currentBatches[batchNum]
			fmt.Println(batch == nil)
			fmt.Printf("batchNum-%v, jobIndex-%v", batchNum, result.job.Index())

			switch {
			// If the query ended because it was canceled, drop it.
			case result.err == ErrJobCanceled:
				log.Tracef("%v Query(%d) was canceled before "+
					"result was available from peer %v", temp,
					result.job.Index(), result.peer.Addr())

				// If this is the first job in this batch that
				// was canceled, forward the error on the
				// batch's error channel.  We do this since a
				// cancellation applies to the whole batch.
				log.Warnf("%v Query(%d) from peer %v cancelled, "+
					"rescheduling: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)

				if batch != nil {
					if batch.errChan != nil {
						batch.errChan <- result.err
					}
					fmt.Println("deleting batch because job cancelled")
					delete(currentBatches, batchNum)

					log.Debugf("Canceled batch %v",
						batchNum)
					continue
				}

			// If the query ended with any other error, put it back
			// into the work queue.
			case result.err != nil:

				log.Warnf("%v Query(%d) from peer %v failed, "+
					"rescheduling: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)

				w.workRWMtx.Lock()
				log.Debugf("%v Query(%d) from peer %v failed, "+
					"locking to write into work: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)
				//newJob := result.job
				//newRequest := *result.job.Request
				//newJob.Request = &newRequest
				heap.Push(w.work, &result.job)
				log.Debugf("%v Query(%d) from peer %v, "+
					"on my way to unlocking to write out of work: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)
				w.workRWMtx.Unlock()
				currentQueries[result.job.Index()] = batchNum

				// Otherwise we got a successful result and  update the
				// status of the batch this query is a part of.
			default:

				if result.unFinished && batch != nil {
					result.job.index = result.job.Index() + 0.0005
					log.Debugf("%v job %v is unfinished, creating new index", result.job.Index())
					log.Debugf("Length of testWork before push %v", temp, w.work.Len())
					w.workRWMtx.Lock()
					fmt.Println("heap pushing")
					heap.Push(w.work, &result.job)
					fmt.Println("In -- batch.rem")
					batch.rem++
					fmt.Println("batch.rem")
					log.Debugf("%v Length of testWork after push %v", temp, w.work.Len())
					fmt.Println("peeking")
					tem := w.work.Peek().(*QueryJob)
					w.workRWMtx.Unlock()
					log.Debugf("%v First element in the heap: %v", temp, tem.Index())
					currentQueries[result.job.Index()] = batchNum

				} else {
					log.Debugf("%v job %v is Finished", temp, result.job.Index())
				}
				// Decrement the number of queries remaining in
				// the batch.
				if batch != nil {
					batch.rem--
					log.Debugf("%v Remaining jobs for batch "+
						"%v: %v ", temp, batchNum, batch.rem)

					// If this was the last query in flight
					// for this batch, we can notify that
					// it finished, and delete it.
					if batch.rem == 0 {
						log.Debugf("batch.errchan == nil, %v", batch.errChan == nil)
						if batch.errChan != nil {
							batch.errChan <- result.err
						}
						fmt.Println("deleting batch remaining zeo")
						delete(currentBatches, batchNum)

						log.Debugf("%v Batch done %v ", temp,
							batchNum)

					}
				}
			}

			// If the total timeout for this batch has passed,
			// return an error.
			if batch != nil && !batch.noTimeout {
				select {
				case <-batch.timeout:
					if batch.errChan != nil {
						batch.errChan <- ErrQueryTimeout
					}
					fmt.Println("deleting batch because timeout")
					delete(currentBatches, batchNum)

					log.Warnf("Query(%d) failed with "+
						"error: %v. Timing out.",
						result.job.Index(), result.err)

					log.Debugf("Batch %v timed out",
						batchNum)

				default:
					log.Debugf("%v In default statement for result chan wmgr", temp)
				}
			}

			// A new batch of queries where scheduled.
		// A new batch of queries where scheduled.
		case batch := <-w.newBatches:
			// Add all new queries in the batch to our work queue,
			// with priority given by the order they were
			// scheduled.
			log.Debugf("(%s)Adding new batch(%d) of %d queries to "+
				"work queue", temp, batchIndex, len(batch.requests))
			fmt.Println(len(batch.requests))
			for i, q := range batch.requests {
				fmt.Println(i)
				fmt.Println("Locking to to put job")
				w.workRWMtx.Lock()
				heap.Push(w.work, &QueryJob{
					index:      queryIndex,
					encoding:   batch.options.encoding,
					cancelChan: batch.options.cancelChan,
					Request:    q,
				})
				w.workRWMtx.Unlock()
				fmt.Println("Out of lock for job")

				currentQueries[queryIndex] = batchIndex
				queryIndex++
			}
			fmt.Println("Creating current batches")
			currentBatches[batchIndex] = &batchProgress{
				timeout:   time.After(batch.options.timeout),
				rem:       len(batch.requests),
				errChan:   batch.options.errChan,
				noTimeout: batch.options.noTimeout,
			}
			batchIndex++
			fmt.Println("Out of current batches")
		case <-w.quit:
			log.Debugf("Received the quit channel")
			return
		}
	}
}

// distributeBlkHeaderWork distributes getheader work gotten from the
// blkHeaderDispatcher

func (w *WorkManager) distributeWorkFunc(eligibleWorkerFunc func(*activeWorker, *QueryJob) bool) func() {

	return func() {
		temp := w.cfg.Temp
		log.Debugf("starting distrubute work for %s", temp)
		defer w.wg.Done()
		defer func() {

			log.Debugf("Exited distributeWorkFunc %v", temp)
		}()

	Loop:
		for {

			// If the work queue is non-empty, we'll take out the first
			// element in order to distribute it to a worker.
			//fmt.Println("On If")
			w.workRWMtx.RLock()
			w.workersRWMutex.RLock()
			if w.work.Len() > 0 && len(w.workers) > 0 {
				w.workersRWMutex.RUnlock()
				w.workRWMtx.RUnlock()
				fmt.Println("In loop")
				fmt.Printf("length workers %v\n", len(w.workers))
				fmt.Printf("length work %v\n", w.work.Len())
				w.workRWMtx.Lock()
				next := w.work.Peek().(*QueryJob)

				// Find the peers with free work slots available.
				var eligibleWorkers []BlkHdrPeer
				w.workersRWMutex.RLock()
				for _, r := range w.workers {
					w.workersRWMutex.RUnlock()

					if !eligibleWorkerFunc(r, next) {
						w.workersRWMutex.RLock()
						continue
					}

					eligibleWorkers = append(eligibleWorkers, r.w.Peer())
					log.Debugf("Num eligible worker: %v, for %v, worker length %v", temp, len(eligibleWorkers), w.work.Len())
					w.workersRWMutex.RLock()
				}
				w.workersRWMutex.RUnlock()

				// Use the historical data (last request duration) to rank them.
				w.cfg.Order(eligibleWorkers)

				// Give the job to the highest ranked peer with free
				// slots available.
				//log.Debugf("Trying to give eligible worker work: Num eligible worker: %v", len(eligibleWorkers))
				for _, p := range eligibleWorkers {

					w.workersRWMutex.RLock()
					r := w.workers[p.Addr()]
					w.workersRWMutex.RUnlock()

					// The worker has free work slots, it should
					// pick up the query.
					log.Debugf("Giving Next job to peer: %v for %v", temp, r.w.Peer())
					fmt.Printf("Giving next job to peer %v\n", r.w.Peer().LastReqDuration())
					select {
					case r.w.NewJob() <- next:
						log.Debugf("Sent job %v to worker %v, for %v",
							temp, next.Index(), p)
						fmt.Println("Sent next job to peer")

						heap.Pop(w.work)
						fmt.Println("popping work")
						w.workRWMtx.Unlock()

						r.activeJob = next

						// Go back to start of loop, to check
						// if there are more jobs to
						// distribute.
						continue Loop

					// Remove workers no longer active.
					case <-r.onExit:
						fmt.Println("On exit")
						w.workersRWMutex.Lock()
						delete(w.workers, p.Addr())
						w.workersRWMutex.Unlock()
						log.Debugf("Worker %v has exited %v", r.w.Peer().Addr(), temp)
						continue

					case <-w.quit:
						w.workRWMtx.Unlock()
						return
					}

				}
				w.workRWMtx.Unlock()
			} else {
				w.workersRWMutex.RUnlock()
				w.workRWMtx.RUnlock()
				//fmt.Println("Else now")
			}

		}
	}
}

func IsWorkerEligibleForBlkHdrFetch(r *activeWorker, next *QueryJob) bool {
	// Only one active job at a time is currently
	// supported.

	if r.activeJob != nil {
		return false
	}

	if !r.w.Peer().IsSyncCandidate() {
		return false
	}

	if r.w.Peer().IsPeerBehindStartHeight(next.Req) {
		return false

	}

	return true

}

func IsWorkerEligibleForCFHdrFetch(r *activeWorker, _ *QueryJob) bool {
	// Only one active job at a time is currently
	// supported.
	if r.activeJob != nil {
		return false
	}

	return true

}

// OrderPeers sorts the given peers according to the duration of its last request i.e. time
// from request to response. Peers with lower duration come before those with a higher duration.
func OrderPeers(peers []BlkHdrPeer) {
	sort.Slice(peers, func(i, j int) bool {

		return peers[i].LastReqDuration() < peers[j].LastReqDuration()
	})
}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTO: Part of the Dispatcher interface.
func (w *WorkManager) Query(requests []*Request,
	options ...QueryOption) chan error {
	var req string
	if requests[0].HandleResp == nil {
		req = "blk headers"
		log.Debugf("sending query to work manager for %s", req)
	} else {
		log.Debugf("sending query to work manager for , %s", req)
	}
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	newBatch := &batch{
		requests: requests,
		options:  qo,
	}
	// Add query messages to the queue of batches to handle.
	select {
	case w.newBatches <- newBatch:
		log.Debugf("New batch sent for , %s", req)
	case <-w.quit:
		if newBatch.options.errChan != nil {
			newBatch.options.errChan <- ErrWorkManagerShuttingDown
		}
	}
	log.Debugf("Out of sending query for %s", req)
	return newBatch.options.errChan
}
