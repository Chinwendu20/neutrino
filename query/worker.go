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
)

// QueryJob is the internal struct that wraps the Query to work on, in
// addition to some information about the query.
type QueryJob struct {
	index      float64
	encoding   wire.MessageEncoding
	cancelChan <-chan struct{}
	*Request
}

// BlkHdrQueryJob should satisfy the Task interface in order to be sorted by the
// workQueue.
var _ Task = (*QueryJob)(nil)

// Index returns the QueryJob's index within the work queue.
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
	job        QueryJob
	peer       BlkHdrPeer
	err        error
	unFinished bool
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

	//feedBackChan is the channel with which error is returned, if any while sending query
	// in worker
	feedBackChan chan error

	// debugName is the Name of the worker instance
	debugName string
}

// A compile-time check to ensure worker satisfies the Worker interface.
var _ Worker = (*worker)(nil)

// NewWorker creates a new worker associated with the given peer.
func NewWorker(peer BlkHdrPeer, name string) Worker {
	return &worker{
		peer:         peer,
		nextJob:      make(chan *QueryJob),
		debugName:    name,
		feedBackChan: make(chan error),
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

	// Subscribe to messages from the peer.
	msgChan, cancel := peer.SubscribeRecvMsg()
	defer cancel()
	defer func() {
		fmt.Println("Exiting worker")
	}()

	for {
		log.Debugf("%v Worker %v waiting for more work for %v", w.debugName, peer.Addr())

		var job *QueryJob
		fmt.Println("Worker running")
		select {
		// Poll a new job from the nextJob channel.
		case job = <-w.nextJob:
			log.Tracef("%v Worker %v picked up job with index %v for %v",
				w.debugName, peer.Addr(), job.Index())
			fmt.Println("received job")
		// Ignore any message received while not working on anything.
		case msg := <-msgChan:
			log.Tracef("%v Worker %v ignoring received msg %T "+
				"since no job active for %v", w.debugName, peer.Addr(), msg)
			fmt.Println("received msg")
			continue

		// If the peer disconnected, we can exit immediately, as we
		// weren't working on a query.
		case <-peer.OnDisconnect():
			log.Debugf("%v peer %v for worker disconnected for %v",
				w.debugName, peer.Addr())
			fmt.Println("disconnected")
			return

		case <-quit:
			fmt.Println("quit")
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
				"already canceled for %v", w.debugName, peer.Addr(), job.Index())

			// We break to the below loop, where we'll check the
			// cancel channel again and the ErrJobCanceled
			// result will be sent back.

			break

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Debugf("%v Worker %v queuing job %T with index %v for %v",
				w.debugName, peer.Addr(), job.Req, job.Index())

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

		}

		queryTimeout := peer.PeerTimeout()
		if peer.LastReqDuration() > MinQueryTimeout &&
			peer.LastReqDuration() < MaxQueryTimeout {

			fmt.Println("\n\nHeeeere")
			queryTimeout = peer.LastReqDuration()
		}
		log.Debugf("%v Timeout for job is %v for %v", w.debugName, queryTimeout)
		fmt.Printf("\nTimeout for job %v. Peer timeout %v", timeout, peer.PeerTimeout())
		timeout = time.NewTimer(queryTimeout)
		fmt.Printf("\nTimeout for job %v.", timeout)

		// Wait for the correct response to be received from the peer,
		// or an error happening.
	feedbackLoop:
		for {
			fmt.Println("In feedback loop")
			select {
			// A message was received from the peer, use the
			// response handler to check whether it was answering
			// our request.
			case resp := <-msgChan:
				fmt.Println("Gotten response")
				progress := job.HandleResp(
					job.Req, resp, peer.Addr(),
				)

				log.Debugf("Worker %v handled msg %T while "+
					"waiting for response to %T (job=%v). "+
					"Finished=%v, progressed=%v, for %v",
					peer.Addr(), resp, job.Req, job.Index(),
					progress.Finished, progress.Progressed, w.debugName)

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

						break feedbackLoop
					}
					fmt.Println("Continuing loop")
					continue feedbackLoop
				}

				// We did get a valid response, and can break
				// the loop.
				break feedbackLoop
			case feedbackErr := <-w.feedBackChan:
				fmt.Printf("\n feedback err: %v", feedbackErr)

				if feedbackErr != ErrResponseExistForQuery {
					jobErr = feedbackErr
				}
				break feedbackLoop
			// If the timeout is reached before a valid response
			// has been received, we exit with an error.
			case <-timeout.C:
				fmt.Println("\n Timeout")
				// The query did experience a timeout and will
				// be given to someone else.
				jobErr = ErrQueryTimeout
				log.Debugf("Worker %v timeout for request %T "+
					"with job index %v for %v", peer.Addr(),
					job.Req, job.Index(), w.debugName)

				break feedbackLoop

			// If the peer disconnects before giving us a valid
			// answer, we'll also exit with an error.
			case <-peer.OnDisconnect():
				fmt.Println("\n Disconnected")
				log.Debugf("peer %v for worker disconnected, "+
					"cancelling job %v, for %v", peer.Addr(),
					job.Index(), w.debugName)

				if jobErr == ErrQueryTimeout {

					peer.UpdateRequestDuration()

				}

				jobErr = ErrPeerDisconnected
				break feedbackLoop

			// If the job was canceled, we report this back to the
			// work manager.
			case <-job.cancelChan:
				fmt.Println("\n Cancelled")
				log.Tracef("Worker %v job %v canceled for %v",
					peer.Addr(), job.Index(), w.debugName)

				jobErr = ErrJobCanceled
				break feedbackLoop

			case <-quit:
				return
			}
		}
		fmt.Println("\n Out of")
		// Stop to allow garbage collection.
		timeout.Stop()

		// We have a result ready for the query, hand it off before
		// getting a new job.

		newJob := QueryJob{
			index: job.Index(),
			Request: &Request{
				Req:        job.CloneReq(job.Req),
				HandleResp: job.Request.HandleResp,
				CloneReq:   job.Request.CloneReq,
				SendQuery:  job.Request.SendQuery,
			},
			encoding:   job.encoding,
			cancelChan: job.cancelChan,
		}
		log.Debugf("In worker checking req immutable %v", newJob == *job)

		select {
		case results <- &jobResult{
			job:        newJob,
			peer:       peer,
			err:        jobErr,
			unFinished: jobUnfinished,
		}:
			log.Debugf("SendResultToWorkMgr received result")
		case <-quit:
			return
		}

		log.Debugf("%v Is job unfinished, %v from %v, index=%v", w.debugName, jobUnfinished, peer.Addr(), job.index)

		if jobErr == ErrQueryTimeout {
			log.Debugf("%v going back to feedback loop after timeout %v %v %v", w.debugName, peer.Addr(), job.index)
			jobErr = nil

			goto feedbackLoop
		}

		// If the peer disconnected, we can exit immediately.
		if jobErr == ErrPeerDisconnected {
			log.Debugf("%v return after peeer disconnected %v %v %v", w.debugName, peer.Addr(), job.index)
			return
		}

		fmt.Println("\n new job")
	}
}
