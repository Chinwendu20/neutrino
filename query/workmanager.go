package query

import (
	"container/heap"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/wire"
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

	Peer() Peer
}

// PeerRanking is an interface that must be satisfied by the underlying module
// that is used to determine which peers to prioritize querios on.
type PeerRanking interface {

	// Order sorst the slice of peers according to their ranking.
	Order(peers []Peer)
}

type queryProgress struct {
	batchIndex uint64
	completed  bool
	job        *QueryJob
}

// activeWorker wraps a Worker that is currently running, together with the job
// we have given to it.
// TODO(halseth): support more than one active job at a time.
type activeWorker struct {
	w         Worker
	activeJob *QueryJob
	onExit    chan struct{}
}

type singleQuery struct {
	index  float64
	newReq wire.Message
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
	NewWorker func(Peer, string) Worker

	// Order is used to rank the connected peers when determining who to
	// give work to.
	Order func(peers []Peer)

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

	workers        map[string]*activeWorker
	work           *workQueue
	newSingleQuery chan *singleQuery
}

// Compile time check to ensure WorkManager satisfies the Dispatcher interface.
var _ Dispatcher = (*WorkManager)(nil)

// New returns a new WorkManager with the regular worker implementation.
func New(cfg *Config) *WorkManager {
	return &WorkManager{
		cfg:            cfg,
		newBatches:     make(chan *batch),
		jobResults:     make(chan *jobResult, 1),
		quit:           make(chan struct{}),
		workers:        make(map[string]*activeWorker),
		work:           &workQueue{},
		newSingleQuery: make(chan *singleQuery),
	}
}

// Start starts the WorkManager.
func (w *WorkManager) Start() error {
	// Init a work queue which will be used to sort the incoming queries in
	// a first come first served fashion. We use a heap structure such
	// that we can efficiently put failed queries back in the queue.

	log.Debugf("starting wormanager for %s", w.cfg.Temp)
	fmt.Printf("starting wormanager for %s", w.cfg.Temp)
	heap.Init(w.work)

	w.wg.Add(2)
	go w.workDispatcher()
	go w.distributeWorkFunc(w.cfg.IsEligibleWorkerFunc)()

	return nil
}

// Stop stops the WorkManager and all underlying goroutines.
func (w *WorkManager) Stop() error {
	fmt.Printf("stoppin wm %v \n", w.cfg.Temp)
	close(w.quit)
	w.wg.Wait()
	fmt.Printf("stopped wm %v \n", w.cfg.Temp)
	return nil
}

// workDispatcher receives batches of queries to be performed from external
// callers, and dispatches these to active workers.  It makes sure to
// prioritize the queries in the order they come in, such that early queries
// will be attempted completed first.
//
// NOTE: MUST be run as a goroutine.
func (w *WorkManager) workDispatcher() {
	defer func() {
		fmt.Println("Exited work dispatcher")
	}()
	temp := w.cfg.Temp
	log.Debugf("Inside work dispatcher for %s", temp)
	fmt.Printf("Inside work dispatcher for %s\n", temp)
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
		fmt.Printf("%v Out of work dispatcher\n", temp)
		for _, b := range currentBatches {
			if b.errChan != nil {
				fmt.Printf("%v sending wm shutting down err\n", temp)
				//b.errChan <- ErrWorkManagerShuttingDown
				fmt.Printf("%v send wm shutting down err\n", temp)
			}
		}
	}()

	// We set up a counter that we'll increase with each incoming query,
	// and will serve as the priority of each. In addition we map each
	// query to the batch they are part of.
	queryIndex := float64(0)
	currentQueries := make(map[float64]*queryProgress)
	retryQueries := make(map[float64]*queryProgress)

	for {
		// Otherwise the work queue is empty, or there are no workers
		// to distribute work to, so we'll just wait for a result of a
		// previous query to come back, a new peer to connect, or for a
		// new batch of queries to be scheduled.
		log.Debugf("%v Waiting for ", temp)
		fmt.Printf("%v Waiting for \n", temp)
		select {
		// Spin up a goroutine that runs a worker each time a peer
		// connects.
		case connectPeer := <-peersConnected:
			fmt.Println("WM peers connected")
			peer := connectPeer.(Peer)
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
			fmt.Println("WM job results")

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
			var queryProg *queryProgress
			queryRetryProg, ok := retryQueries[result.job.Index()]

			queryCurrentProg, ok := currentQueries[result.job.Index()]
			if !ok {

				continue
			}
			if queryCurrentProg != nil && queryCurrentProg.completed {

				if queryCurrentProg == nil || queryCurrentProg.completed {
					log.Debugf("job %v not found/completed in both current and retry", result.job.index)
					continue
				}

				queryProg = queryRetryProg

				continue

			} else {
				queryProg = queryCurrentProg
			}

			queryProg.completed = true
			batch, ok := currentBatches[queryProg.batchIndex]

			if !ok {
				log.Debugf("%vBatch for job %v does not exist %v "+
					"(err=%v). Batch no: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err, queryProg.batchIndex)
				continue
			}

			switch {
			// If the query ended because it was canceled, drop it.
			case result.err == ErrJobCanceled:
				log.Tracef("%v QueryBatch(%d) was canceled before "+
					"result was available from peer %v", temp,
					result.job.Index(), result.peer.Addr())

				// If this is the first job in this batch that
				// was canceled, forward the error on the
				// batch's error channel.  We do this since a
				// cancellation applies to the whole batch.
				log.Warnf("%v QueryBatch(%d) from peer %v cancelled, "+
					"rescheduling: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)

				if batch.errChan != nil {
					fmt.Println("326 batch err")
					batch.errChan <- result.err
					fmt.Println("326 out batch err")
				}
				log.Debugf("Deleting because of batch cancelled")
				delete(currentBatches, queryProg.batchIndex)

				log.Debugf("Canceled batch %v",
					queryProg.batchIndex)
				continue

			// If the query ended with any other error, put it back
			// into the work queue.
			case result.err != nil:

				log.Warnf("%v QueryBatch(%d) from peer %v failed, "+
					"rescheduling: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)

				w.workRWMtx.Lock()
				log.Debugf("%v QueryBatch(%d) from peer %v failed, "+
					"locking to write into work: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)
				//newReq := result.job
				//newRequest := *result.job.Request
				//newReq.Request = &newRequest
				fmt.Printf("Pushingwork as a result of error: %v, index-%v, Peer %v \n", result.err, result.job.Index(), result.peer)
				heap.Push(w.work, &result.job)
				log.Debugf("%v QueryBatch(%d) from peer %v, "+
					"on my way to unlocking to write out of work: %v", temp, result.job.Index(),
					result.peer.Addr(), result.err)
				w.workRWMtx.Unlock()
				queryProg.completed = false

				// Otherwise we got a successful result and  update the
				// status of the batch this query is a part of.
			default:

				if result.unFinished {
					result.job.index = result.job.Index() + 0.0005
					log.Debugf("%v job %v is unfinished, creating new index", result.job.Index())
					log.Debugf("Length of testWork before push %v", temp, w.work.Len())
					w.workRWMtx.Lock()
					fmt.Println("Pushing work as result is unfinished")
					heap.Push(w.work, &result.job)
					batch.rem++
					log.Debugf("%v Length of testWork after push %v", temp, w.work.Len())
					tem := w.work.Peek().(*QueryJob)
					w.workRWMtx.Unlock()
					log.Debugf("%v First element in the heap: %v", temp, tem.Index())
					currentQueries[result.job.Index()] = &queryProgress{
						batchIndex: queryProg.batchIndex,
						completed:  false,
						job:        &result.job,
					}

				} else {
					log.Debugf("%v job %v is Finished", temp, result.job.Index())
				}
				// Decrement the number of queries remaining in
				// the batch.

				batch.rem--
				log.Debugf("%v Remaining jobs for batch "+
					"%v: %v ", temp, queryProg.batchIndex, batch.rem)

				// If this was the last query in flight
				// for this batch, we can notify that
				// it finished, and delete it.
				if batch.rem == 0 {
					log.Debugf("batch.errchan == nil, %v", batch.errChan == nil)
					fmt.Printf("batch.errchan == nil, %v\n", batch.errChan == nil)
					if batch.errChan != nil {
						fmt.Println("398 batch err")
						batch.errChan <- result.err
						fmt.Println("398 out batch err")
					}

					log.Debugf("%v Batch done %v ", temp,
						queryProg.batchIndex)
				}
			}

			// If the total timeout for this batch has passed,
			// return an error.
			if !batch.noTimeout {
				select {
				case <-batch.timeout:
					if batch.errChan != nil {
						fmt.Println("415 batch err")
						batch.errChan <- ErrQueryTimeout
						fmt.Println("415 out batch err")
					}
					log.Debugf("Deleting because of batch timeout")
					delete(currentBatches, queryProg.batchIndex)

					log.Warnf("QueryBatch(%d) failed with "+
						"error: %v. Timing out.",
						result.job.Index(), result.err)

					log.Debugf("Batch %v timed out",
						queryProg.batchIndex)

				default:
					log.Debugf("%v In default statement for result chan wmgr", temp)
				}
			}
		case request := <-w.newSingleQuery:
			fmt.Println("WM singlequery")
			query, ok := currentQueries[request.index]
			if !ok {
				query, ok = retryQueries[request.index]
				if !ok {
					log.Errorf("Request query not found in single query")
				}
			}
			b := currentBatches[query.batchIndex]
			req := query.job.Req
			idx := request.index
			oldJob := query.job
			if request.newReq != nil {
				req = request.newReq
				idx = request.index + 0.0005
			}

			job := &QueryJob{
				index:      idx,
				encoding:   oldJob.encoding,
				cancelChan: oldJob.cancelChan,
				Request: &Request{
					Req:        req,
					HandleResp: oldJob.HandleResp,
					CloneReq:   oldJob.CloneReq,
					SendQuery:  oldJob.SendQuery,
				},
			}
			w.workRWMtx.Lock()
			fmt.Println("Pushing as a result of single query")
			heap.Push(w.work, job)
			b.rem++
			log.Debugf("%v Length of testWork after push %v", temp, w.work.Len())
			fmt.Printf("%v Length of testWork after push %v\n", temp, w.work.Len())
			tem := w.work.Peek().(*QueryJob)
			w.workRWMtx.Unlock()
			log.Debugf("%v First element in the heap: %v", temp, tem.Index())
			fmt.Printf("%v First element in the heap: %v\n", temp, tem.Index())
			fmt.Println("Done with singlequery in work disp")
			retryQueries[job.index] = &queryProgress{
				batchIndex: batchIndex,
				completed:  false,
				job:        job,
			}
			// A new batch of queries where scheduled.
		// A new batch of queries where scheduled.
		case batch := <-w.newBatches:
			fmt.Println("WM new batches")
			// Add all new queries in the batch to our work queue,
			// with priority given by the order they were
			// scheduled.
			log.Debugf("(%s)Adding new batch(%d) of %d queries to "+
				"work queue", temp, batchIndex, len(batch.requests))
			fmt.Printf("(%s)Adding new batch(%d) of %d queries to "+
				"work queue\n", temp, batchIndex, len(batch.requests))
			for _, q := range batch.requests {
				w.workRWMtx.Lock()
				job := &QueryJob{
					index:      queryIndex,
					encoding:   batch.options.encoding,
					cancelChan: batch.options.cancelChan,
					Request:    q,
				}
				heap.Push(w.work, job)
				w.workRWMtx.Unlock()
				currentQueries[queryIndex] = &queryProgress{
					batchIndex: batchIndex,
					completed:  false,
					job:        job,
				}
				queryIndex++
			}
			fmt.Printf("0-batch index counter-%v\n", batchIndex)
			currentBatches[batchIndex] = &batchProgress{
				timeout:   time.After(batch.options.timeout),
				rem:       len(batch.requests),
				errChan:   batch.options.errChan,
				noTimeout: batch.options.noTimeout,
			}
			fmt.Printf("1-batch index counter-%v\n", batchIndex)
			batchIndex++
			fmt.Printf("2-batch index counter-%v\n", batchIndex)
		case <-w.quit:
			log.Debugf("Received the quit channel")
			fmt.Println("Received the quit channel work dispatcher")
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
			fmt.Printf("Exited distributeWorkFunc %v\n", temp)
		}()

	Loop:
		for {
			select {
			case <-w.quit:
				return
			default:
				// If the work queue is non-empty, we'll take out the first
				// element in order to distribute it to a worker.
				w.workRWMtx.RLock()
				w.workersRWMutex.RLock()
				if w.work.Len() > 0 && len(w.workers) > 0 {
					w.workersRWMutex.RUnlock()
					w.workRWMtx.RUnlock()
					w.workRWMtx.Lock()
					next := w.work.Peek().(*QueryJob)

					// Find the peers with free work slots available.
					var eligibleWorkers []Peer
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
						fmt.Printf("Giving Next job to peer: (%v) %v for %v\n", next.Index(), temp, r.w.Peer())
						select {
						case r.w.NewJob() <- next:
							log.Debugf("Sent job %v to worker %v, for %v",
								temp, next.Index(), p)
							fmt.Printf("Sent job %v to worker %v, for %v\n",
								temp, next.Index(), p)

							heap.Pop(w.work)
							fmt.Printf("Length of work %v\n", w.work.Len())
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
				}
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
func OrderPeers(peers []Peer) {
	sort.Slice(peers, func(i, j int) bool {

		return peers[i].LastReqDuration() < peers[j].LastReqDuration()
	})
}

// Query distributes the slice of requests to the set of connected peers.
//
// NOTE: Part of the Dispatcher interface.
func (w *WorkManager) QueryBatch(requests []*Request,
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

func (w *WorkManager) QueryOnce(request *RetryRequest) {
	log.Debugf("In queryOnce function")
	newSingleQuery := &singleQuery{
		index:  request.Index,
		newReq: request.NewJob,
	}

	select {
	case w.newSingleQuery <- newSingleQuery:
		log.Debugf("New single query sent for , %s", request.Index)
		fmt.Println("single query sent")
	case <-w.quit:
	}
}
