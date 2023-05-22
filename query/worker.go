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
	tries      uint8
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
	temp     string
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker associated with the given peer.
func NewWorker(peer BlkHdrPeer, work string) Worker {
	return &worker{
		peer:    peer,
		nextJob: make(chan *QueryJob),
		temp:    work,
	}
}

func (w *worker) Peer() BlkHdrPeer {

	return w.peer
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

	for {
		log.Debugf("Worker %v waiting for more work for %v", peer.Addr(), w.temp)

		var job *QueryJob
		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Tracef("Worker %v picked up job with index %v for %v",
				peer.Addr(), job.Index(), w.temp)

		// Ignore any message received while not working on anything.
		case msg := <-msgChan:
			log.Tracef("Worker %v ignoring received msg %T "+
				"since no job active for %v", peer.Addr(), msg, w.temp)
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("Peer %v for worker disconnected for %v",
				peer.Addr(), w.temp)
			return

		case <-quit:
			return
		}

		var (
			jobErr        error
			timeout       *time.Timer
			jobUnfinished bool
		)

		select {
		// There is no point in queueing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.cancelChan:
			log.Tracef("Worker %v found job with index %v "+
				"already canceled for %v", peer.Addr(), job.Index(), w.temp)

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.
			break

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Debugf("Worker %v queuing job %T with index %v for %v",
				peer.Addr(), job.Req, job.Index(), w.temp)

			job.SendQuery(w, job)
			queryTimeout := peer.PeerTimeout()
			if peer.LastReqDuration() > MinQueryTimeout &&
				peer.LastReqDuration() < MaxQueryTimeout {

				queryTimeout = peer.LastReqDuration()
			}
			log.Debugf("Timeout for job is %v for %v", queryTimeout, w.temp)
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

					log.Debugf("Worker %v handled msg %T while "+
						"waiting for response to %T (job=%v). "+
						"Finished=%v, progressed=%v, for %v",
						peer.Addr(), resp, job.Req, job.Index(),
						progress.Finished, progress.Progressed, w.temp)

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
							jobUnfinished = true
							job.JobIndex = job.JobIndex

							break feedbackLoop
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
				log.Debugf("Worker %v timeout for request %T "+
					"with job index %v for %v", peer.Addr(),
					job.Req, job.Index(), w.temp)

				break feedbackLoop

			// If the peer disconnects before giving us a valid
			// answer, we'll also exit with an error.
			case <-peer.OnDisconnect():
				log.Debugf("Peer %v for worker disconnected, "+
					"cancelling job %v, for %v", peer.Addr(),
					job.Index(), w.temp)

				if jobErr == ErrQueryTimeout {

					peer.UpdateRequestDuration()

				}

				jobErr = ErrPeerDisconnected
				break feedbackLoop

			// If the job was canceled, we report this back to the
			// work manager.
			case <-job.cancelChan:
				log.Tracef("Worker %v job %v canceled for %v",
					peer.Addr(), job.Index(), w.temp)

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
		SendResultToWorkMgr(results, &JobResult{
			Job:        *job,
			Peer:       peer,
			Err:        jobErr,
			UnFinished: jobUnfinished,
		}, quit)

		log.Debugf("Is job unfinished, %v from %v, index=%v for %v", jobUnfinished, peer.Addr(), job.JobIndex, w.temp)

		if jobErr == ErrQueryTimeout {
			log.Debugf("going back to feedback loop after timeout %v %v %v", peer.Addr(), job.JobIndex, w.temp)
			jobErr = nil
			goto feedbackLoop
		}

		// If the peer disconnected, we can exit immediately.
		if jobErr == ErrPeerDisconnected {
			log.Debugf("return after peeer disconnected %v %v %v", peer.Addr(), job.JobIndex, w.temp)
			return
		}
	}
}

// BlkManagerQuery contains fields required for
// communication between the blockmanager and
// the worker and workmanager

// TODO(Maureen): Probably move to blockmanager->Remove entirely
type BlkManagerQuery struct {
	RespChan chan<- interface{}
	Job      QueryJob
}

// SendResultToWorkMgr sends feedback to the workmanager, letting it know if it was successful
// or not. It is called by the worker if there are no errors and by the blockmanager if there
// are errors.
func SendResultToWorkMgr(results chan<- *JobResult,
	jobResult *JobResult, quit <-chan struct{}) {

	// We have a result ready for the query, hand it off before
	// getting a new job.
	select {
	case results <- jobResult:
		log.Debugf("SendResultToWorkMgr received result for peer=%v, index=%V", jobResult.Peer.Addr(), jobResult.Job.Index())
	case <-quit:
		return
	}

}
