package query

import (
	"errors"
	"time"

	"github.com/btcsuite/btcd/wire"
)

var (
	// ErrQueryTimeout is an error returned if the worker doesn't respond
	// with a valid response to the request within the timeout.
	ErrQueryTimeout = errors.New("did not get response before timeout")

	// ErrPeerDisconnected is returned if the worker's peer disconnect
	// before the query has been answered.
	ErrPeerDisconnected = errors.New("peer disconnected")

	// ErrJobCanceled is returned if the job is canceled before the query
	// has been answered.
	ErrJobCanceled = errors.New("job canceled")
)

// QueryJob is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type QueryJob struct {
	JobIndex   float64
	timeout    time.Duration
	encoding   wire.MessageEncoding
	cancelChan <-chan struct{}
	*Request
}

// BlkHdrQueryJob should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*QueryJob)(nil)

// JobIndex returns the QueryJob's index within the work queue.
//
// NOTE: Part of the Task interface.
func (q *QueryJob) Index() float64 {
	return q.JobIndex
}

func (q *QueryJob) Encoding() wire.MessageEncoding {
	return q.encoding
}

func (q *QueryJob) JobRequest() wire.Message {
	return q.Req
}

// JobResult is the final result of the worker's handling of the QueryJob.
type JobResult struct {
	Job        QueryJob
	Peer       BlkHdrPeer
	Err        error
	UnFinished bool
}

// worker is responsible for polling work from its work queue, and handing it
// to the associated peer. It validates incoming responses with the current
// query's response handler, and polls more work for the peer when it has
// successfully received a response to the request.
type worker struct {
	peer BlkHdrPeer

	// nextJob is a channel of queries to be distributed, where the worker
	// will poll new work from.
	nextJob chan *QueryJob

	respChan chan interface{}
}

// blkHdrWorker is responsible for polling work from its work queue.
// It interfaces between the work manager and block manager
type blkHdrWorker struct {
	peer BlkHdrPeer

	nextJob chan *BlkHdrQueryJob

	activeJob bool
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker associated with the given peer.
func NewWorker(peer BlkHdrPeer) Worker {
	return &worker{
		peer:    peer,
		nextJob: make(chan *QueryJob),
	}
}

func (w *worker) Peer() BlkHdrPeer {

	return nil
}

// NewJob returns a channel where work that is to be handled by the worker can
// be sent. If the worker reads a QueryJob from this channel, it is guaranteed
// that a response will eventually be deliverd on the results channel (except
// when the quit channel has been closed).
//
// NOTE: Part of the Worker interface.
func (w *worker) NewJob() chan<- *QueryJob {
	return w.nextJob
}

func (w *worker) RespChan() chan<- interface{} {
	return w.respChan
}

// NewBlkHdrWorker creates a new blkHdrWorker associated with the given peer.
func NewBlkHdrWorker(peer BlkHdrPeer) *blkHdrWorker {
	return &blkHdrWorker{
		peer:      peer,
		nextJob:   make(chan *BlkHdrQueryJob),
		activeJob: false,
	}
}

// Peer returns the peer of the worker
func (w *blkHdrWorker) Peer() BlkHdrPeer {

	return w.peer
}

// Run starts the worker. The worker will supply its peer with queries, and
// handle responses from it. Results for any query handled by this worker will
// be delivered on the results channel. quit can be closed to immediately make
// the worker exit.
//
// The method is blocking, and should be started in a goroutine. It will run
// until the peer disconnects or the worker is told to quit.
//
// NOTE: Part of the Worker interface.
func (w *worker) Run(results chan<- *JobResult, quit <-chan struct{}) {
	peer := w.peer

	// Subscribe to messages from the peer.
	msgChan, cancel := peer.SubscribeRecvMsg()
	defer cancel()

nextJobLoop:
	for {
		log.Tracef("Worker %v waiting for more work", peer.Addr())

		var job *QueryJob
		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Tracef("Worker %v picked up job with index %v",
				peer.Addr(), job.Index())

		// Ignore any message received while not working on anything.
		case msg := <-w.respChan:
			log.Tracef("Worker %v ignoring received msg %T "+
				"since no job active", peer.Addr(), msg)
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected",
				peer.Addr())
			return

		case <-quit:
			return
		}

		var (
			jobErr  error
			timeout *time.Timer
		)

		select {
		// There is no point in queueing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.cancelChan:
			log.Tracef("Worker %v found job with index %v "+
				"already canceled", peer.Addr(), job.Index())

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.
			break

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Tracef("Worker %v queuing job %T with index %v",
				peer.Addr(), job.Req, job.Index())

			job.SendQuery(w, job)
			queryTimeout := peer.PeerTimeout()
			if peer.LastReqDuration() > MinQueryTimeout &&
				peer.LastReqDuration() < MaxQueryTimeout {

				queryTimeout = peer.LastReqDuration()
			}
			timeout = time.NewTimer(queryTimeout)

		}

		// Wait for the correct response to be received from the peer,
		// or an error happening.
	feedbackLoop:
		for {
			select {
			// A message was received from the peer, use the
			// response handler to check whether it was answering
			// our request.
			case resp := <-msgChan:
				if job.HandleResp != nil {
					progress := job.HandleResp(
						job.Req, resp, peer.Addr(),
					)

					log.Tracef("Worker %v handled msg %T while "+
						"waiting for response to %T (job=%v). "+
						"Finished=%v, progressed=%v",
						peer.Addr(), resp, job.Req, job.Index(),
						progress.Finished, progress.Progressed)

					// If the response did not answer our query, we
					// check whether it did progress it.
					if !progress.Finished {
						// If it did make progress we reset the
						// timeout. This ensures that the
						// queries with multiple responses
						// expected won't timeout before all
						// responses have been handled.
						// TODO(halseth): separate progress
						// timeout value.
						if progress.Progressed {
							timeout.Stop()
							timeout = time.NewTimer(
								job.timeout,
							)
						}
						continue feedbackLoop
					}
				}

				// We did get a valid response, and can break
				// the loop.
				break feedbackLoop

			// If the timeout is reached before a valid response
			// has been received, we exit with an error.
			case <-timeout.C:
				// The query did experience a timeout and will
				// be given to someone else.
				jobErr = ErrQueryTimeout
				log.Tracef("Worker %v timeout for request %T "+
					"with job index %v", peer.Addr(),
					job.Req, job.Index())

				break feedbackLoop

			// If the peer disconnects before giving us a valid
			// answer, we'll also exit with an error.
			case <-peer.OnDisconnect():
				log.Debugf("Peer %v for worker disconnected, "+
					"cancelling job %v", peer.Addr(),
					job.Index())

				if jobErr == ErrQueryTimeout {

					peer.UpdateRequestDuration()

				}

				jobErr = ErrPeerDisconnected
				break feedbackLoop

			// If the job was canceled, we report this back to the
			// work manager.
			case <-job.cancelChan:
				log.Tracef("Worker %v job %v canceled",
					peer.Addr(), job.Index())

				jobErr = ErrJobCanceled
				break feedbackLoop

			case <-quit:
				return
			}
		}

		// Stop to allow garbage collection.
		timeout.Stop()

		// We have a result ready for the query, hand it off before
		// getting a new job.
		select {
		case results <- &JobResult{
			Job:  job,
			Peer: peer,
			Err:  jobErr,
		}:
		case <-quit:
			return
		}

		if jobErr == ErrQueryTimeout {
			goto feedbackLoop
		}

		// If the peer disconnected, we can exit immediately.
		if jobErr == ErrPeerDisconnected {
			return
		}
	}
}

// BlkManagerQuery contains fields required for
// communication between the blockmanager and
// the worker and workmanager

// TODO(Maureen): Probably move to blockmanager
type BlkManagerQuery struct {
	RespChan chan<- interface{}
	Job      QueryJob
}

// Run starts the worker. The worker will supply its peer with queries, and
// notifies the block manager of the query as well. As soon as the worker receives
// a heaer response for its previous query, it goes to polling for more work.
//
// The method is blocking, and should be started in a goroutine. It will run
// until the peer disconnects or the worker is told to quit.

func (w *blkHdrWorker) Run(results chan<- *BlkHdrJobResult, quit <-chan struct{}) {

	peer := w.peer
	log.Infof("blkHdrWorker is running for, %v", peer.Addr())

	blkQuery := BlkManagerQuery{
		RespChan: make(chan struct{}),
		Results:  results,
	}

nextJobLoop:
	var (
		job     *BlkHdrQueryJob
		jobErr  error
		timeout *time.Timer
	)
	for {
		log.Debugf("Worker %v waiting for more work", peer.Addr())

		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Debugf("Worker %v picked up job with index %v",
				peer.Addr(), job.Index())

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected",
				peer.Addr())
			return

		case <-quit:
			return
		}

		select {
		// There is no point in queueing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.cancelChan:
			log.Tracef("Worker %v found job with index %v "+
				"already canceled", peer.Addr(), job.Index())

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.
			return

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Debugf("Worker %v queuing job %T with index %v",
				peer.Addr(), job.ReqDetails, job.Index())

			blkQuery.Job = *job
			job.SendQueryToBlkMgr(
				peer, blkQuery,
			)
			queryTimeout := peer.PeerTimeout()
			if peer.LastReqDuration() > MinQueryTimeout &&
				peer.LastReqDuration() < MaxQueryTimeout {

				queryTimeout = peer.LastReqDuration()
			}
			timeout = time.NewTimer(queryTimeout)
			goto feedbackLoop
		}
	}

	//	Loop to wait from feedback from block manager
feedbackLoop:
	// Wait for the correct response to be received from the peer,
	// or an error happening.

	for {
		select {
		// A header has been received for the query.
		case <-blkQuery.RespChan:
			log.Debugf("Worker gotten respchan, peer=%v for job index %v", peer.Addr(), job.Index())
			w.activeJob = false

			// We did get a header, so we go back to fetching for
			// more work
			goto nextJobLoop
		case <-timeout.C:
			// The query did experience a timeout and will
			// be given to someone else.
			jobErr = ErrQueryTimeout
			log.Tracef("Worker %v timeout for request %T "+
				"with job index %v", peer.Addr(),
				job.BlkHdrRequest, job.Index())
		// If the peer disconnects before giving us a valid
		// answer, we'll also exit with an error.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected, "+
				"cancelling job %v", peer.Addr(),
				job.Index())
			if jobErr == ErrQueryTimeout {

				peer.UpdateRequestDuration()

			}

			jobErr = ErrPeerDisconnected
			//blkQuery.RespChan <- struct{}{}

		// If the job was canceled, we report this back to the
		// work manager.
		case <-job.cancelChan:
			log.Tracef("Worker %v job %v canceled",
				peer.Addr(), job.Index())

			jobErr = ErrJobCanceled

		case <-quit:
			return
		}
		// Stop to allow garbage collection .
		timeout.Stop()

		//Send error result from worker to workManager.
		log.Debugf("SendResultToWorkMgr from worker loop")
		SendResultToWorkMgr(results, &BlkHdrJobResult{
			Job:  *job,
			Peer: peer,
			Err:  jobErr,
		}, quit)

		// If the peer disconnected, we can exit immediately.
		if jobErr == ErrPeerDisconnected {
			return
		}

		if jobErr == ErrQueryTimeout {
			goto feedbackLoop
		}
		// If it is not a peer disconnect continue polling for work
		goto nextJobLoop
	}

}

// SendResultToWorkMgr sends feedback to the workmanager, letting it know if it was successful
// or not. It is called by the worker if there are no errors and by the blockmanager if there
// are errors.
func SendResultToWorkMgr(results chan<- *BlkHdrJobResult,
	jobResult *BlkHdrJobResult, quit <-chan struct{}) {

	// We have a result ready for the query, hand it off before
	// getting a new job.
	select {
	case results <- jobResult:
		log.Debugf("SendResultToWorkMgr received result for peer=%v, index=%V", jobResult.Peer.Addr(), jobResult.Job.Index())
	case <-quit:
		return
	}

}

// NewJob returns a channel where work that is to be handled by the worker can
// be sent. If the worker reads a BlkHdrQueryJob  from this channel, it is guaranteed
// that a response will eventually be delivered on the results channel (except
// when the quit channel has been closed).
func (w *blkHdrWorker) NewJob() chan<- *BlkHdrQueryJob {

	return w.nextJob
}
