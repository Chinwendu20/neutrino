package query

import (
	"errors"
	"fmt"
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

	// ErrResponseExistForQuery is returned while sending query if the response for
	// the query has been received before.
	ErrResponseExistForQuery = errors.New("response Exists for query")

	ErrResponseErr = errors.New("response did not pass preliminary check")
)

// QueryJob is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type QueryJob struct {
	tries      uint8
	index      float64
	timeout    time.Duration
	encoding   wire.MessageEncoding
	cancelChan <-chan struct{}
	*Request
}

// QueryJob should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*QueryJob)(nil)

// Index returns the queryJob's index within the work queue.
//
// NOTE: Part of the Task interface.
func (q *QueryJob) Index() float64 {
	return q.index
}

func (q *QueryJob) Encoding() wire.MessageEncoding {
	return q.encoding
}

// jobResult is the final result of the worker's handling of the QueryJob.
type jobResult struct {
	job        *QueryJob
	peer       Peer
	err        error
	unfinished bool
}

// worker is responsible for polling work from its work queue, and handing it
// to the associated peer. It validates incoming responses with the current
// query's response handler, and polls more work for the peer when it has
// successfully received a response to the request.
type worker struct {
	peer Peer

	// nextJob is a channel of queries to be distributed, where the worker
	// will poll new work from.
	nextJob      chan *QueryJob
	debugName    string
	feedBackChan chan error
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker associated with the given peer.
func NewWorker(peer Peer, debugName string) Worker {
	return &worker{
		peer:         peer,
		nextJob:      make(chan *QueryJob),
		debugName:    debugName,
		feedBackChan: make(chan error),
	}
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
func (w *worker) Run(results chan<- *jobResult, quit <-chan struct{}) {
	peer := w.peer
	name := w.debugName
	// Subscribe to messages from the peer.
	msgChan, cancel := peer.SubscribeRecvMsg()
	defer cancel()

	for {
		log.Tracef("%v Worker %v waiting for more work", name, peer.Addr())

		var job *QueryJob
		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Tracef("%v Worker %v picked up job with index %v", name,
				peer.Addr(), job.Index())

		// Ignore any message received while not working on anything.
		case msg := <-msgChan:
			log.Tracef("%v Worker %v ignoring received msg %T "+
				"since no job active", name, peer.Addr(), msg)
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("%v Peer %v for worker disconnected", name,
				peer.Addr())
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
			log.Tracef("%v Worker %v found job with index %v "+
				"already canceled", name, peer.Addr(), job.Index())

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.
			break

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Tracef("%v Worker %v queuing job %T with index %v", name,
				peer.Addr(), job.Req, job.Index())

			err := job.SendQuery(w, job)
			if err != nil {
				go func() {
					select {
					case w.feedBackChan <- err:
					case <-job.cancelChan:
					case <-quit:
					}
				}()
			}

			queryTimeOut := minQueryTimeout
			if peer.LastReqDuration() > minQueryTimeout && peer.LastReqDuration() <= maxQueryTimeout {
				queryTimeOut = peer.LastReqDuration()
			}
			timeout = time.NewTimer(queryTimeOut)

		Loop:
			for {
				select {
				// A message was received from the peer, use the
				// response handler to check whether it was answering
				// our request.
				case resp := <-msgChan:
					progress := job.HandleResp(
						job.Req.Message, resp, peer, &jobErr,
					)

					log.Tracef("%v Worker %v handled msg %T while "+
						"waiting for response to %T (job=%v). "+
						"Finished=%v, progressed=%v", name,
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
								queryTimeOut,
							)
						}

						if jobErr != nil {
							fmt.Printf("%v Should not in test 210", w.debugName)
							break Loop
						}
						continue Loop
					}
					if !progress.Progressed {
						fmt.Printf("%v Should not in test 216", w.debugName)
						jobUnfinished = true

					}

					log.Debugf("Error response received, %v\n", jobErr)

					// We did get a valid response, and can break
					// the loop.
					break Loop

				// If the timeout is reached before a valid response
				// has been received, we exit with an error.
				case <-timeout.C:
					// The query did experience a timeout and will
					// be given to someone else.
					jobErr = ErrQueryTimeout
					log.Tracef("%v Worker %v timeout for request %T "+
						"with job index %v", name, peer.Addr(),
						job.Req, job.Index())

					break Loop

				case feedbackErr := <-w.feedBackChan:

					//if feedbackErr != ErrResponseExistForQuery {
					jobErr = feedbackErr
					//}
					break Loop

				// If the peer disconnects before giving us a valid
				// answer, we'll also exit with an error.
				case <-peer.OnDisconnect():
					log.Debugf("%v Peer %v for worker disconnected, "+
						"cancelling job %v", name, peer.Addr(),
						job.Index())

					jobErr = ErrPeerDisconnected
					break Loop

				// If the job was canceled, we report this back to the
				// work manager.
				case <-job.cancelChan:
					log.Tracef("Worker %v job %v canceled",
						peer.Addr(), job.Index())

					jobErr = ErrJobCanceled
					break Loop

				case <-quit:
					return
				}
			}

			// Stop to allow garbage collection.
			timeout.Stop()
			resultJob := job

			resultJob = &QueryJob{
				index: job.Index(),
				Request: &Request{
					Req:        job.CloneReq(*job.Req),
					HandleResp: job.Request.HandleResp,
					CloneReq:   job.Request.CloneReq,
					SendQuery:  job.Request.SendQuery,
				},
				encoding:   job.encoding,
				cancelChan: job.cancelChan,
			}
			// We have a result ready for the query, hand it off before
			// getting a new job.
			select {
			case results <- &jobResult{
				job:        resultJob,
				peer:       peer,
				err:        jobErr,
				unfinished: jobUnfinished,
			}:
			case <-quit:
				return
			}
			if jobErr == ErrQueryTimeout {
				jobErr = nil

				goto Loop
			}
			// If the peer disconnected, we can exit immediately.
			if jobErr == ErrPeerDisconnected {
				return
			}
		}
	}
}

// NewJob returns a channel where work that is to be handled by the worker can
// be sent. If the worker reads a queryJob from this channel, it is guaranteed
// that a response will eventually be deliverd on the results channel (except
// when the quit channel has been closed).
//
// NOTE: Part of the Worker interface.
func (w *worker) NewJob() chan<- *QueryJob {
	return w.nextJob
}

func (w *worker) Peer() Peer {
	return w.peer
}
