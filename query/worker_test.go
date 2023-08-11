package query

//
//import (
//	"errors"
//	"fmt"
//	"github.com/btcsuite/btcd/wire"
//	"testing"
//	"time"
//)
//
//var (
//	req          = &wire.MsgGetData{}
//	progressResp = &wire.MsgTx{
//		Version: 111,
//	}
//	finalResp = &wire.MsgTx{
//		Version: 222,
//	}
//)
//
//type mockPeer struct {
//	addr          string
//	requests      chan wire.Message
//	responses     chan<- wire.Message
//	subscriptions chan chan wire.Message
//	quit          chan struct{}
//	err           error
//	syncCandidate bool
//	behindPeer    bool
//	reqDuration   time.Duration
//	peerTimeout   time.Duration
//}
//
//var _ Peer = (*mockPeer)(nil)
//
//func (m *mockPeer) QueueMessageWithEncoding(msg wire.Message,
//	_ chan<- struct{}, _ wire.MessageEncoding) {
//
//	m.requests <- msg
//}
//
//func (m *mockPeer) SubscribeRecvMsg() (<-chan wire.Message, func()) {
//	msgChan := make(chan wire.Message)
//
//	m.subscriptions <- msgChan
//	return msgChan, func() {}
//}
//
//func (m *mockPeer) OnDisconnect() <-chan struct{} {
//	return m.quit
//}
//
//func (m *mockPeer) Addr() string {
//	return m.addr
//}
//func (m *mockPeer) IsPeerBehindStartHeight(_ wire.Message) bool {
//
//	return m.behindPeer
//
//}
//
//func (m *mockPeer) LastReqDuration() time.Duration {
//
//	return m.reqDuration
//}
//
//func (m *mockPeer) UpdateRequestDuration() {
//}
//
//func (m *mockPeer) PeerTimeout() time.Duration {
//
//	return m.peerTimeout
//}
//
//func (m *mockPeer) IsSyncCandidate() bool {
//	return m.syncCandidate
//}
//
//// makeJob returns a new query job that will be done when it is given the
//// finalResp message. Similarly ot will progress on being given the
//// progressResp message, while any other message will be ignored.
//func makeJob() *QueryJob {
//	q := &Request{
//		Req: req,
//		HandleResp: func(req, resp wire.Message, _ string, _ float64) Progress {
//			if resp == finalResp {
//				return Progress{
//					Finished:   true,
//					Progressed: true,
//				}
//			}
//
//			if resp == progressResp {
//				return Progress{
//					Finished:   false,
//					Progressed: true,
//				}
//			}
//
//			return Progress{
//				Finished:   false,
//				Progressed: false,
//			}
//		},
//
//		SendQuery: func(work Worker, task Task) error {
//
//			m := work.Peer().(*mockPeer)
//
//			if m.err != nil {
//				return m.err
//			}
//			job := task.(*QueryJob)
//			m.requests <- job.Req
//			return nil
//		},
//		CloneReq: func(message wire.Message) wire.Message {
//			msg := message.(*wire.MsgGetData)
//			clone := &wire.MsgGetData{
//				InvList: msg.InvList,
//			}
//
//			return clone
//		},
//	}
//	return &QueryJob{
//		index:      123,
//		encoding:   defaultQueryEncoding,
//		cancelChan: nil,
//		Request:    q,
//	}
//}
//
//type testCtx struct {
//	nextJob    chan<- *QueryJob
//	jobResults chan *jobResult
//	peer       *mockPeer
//	workerDone chan struct{}
//}
//
//// startWorker creates and starts a worker for a new mockPeer. A getHdrBatch context
//// containing channels to hand the worker new jobs and receiving the job
//// results, in addition to the mockPeer, is returned.
//func startWorker() (*testCtx, error) {
//	peer := &mockPeer{
//		requests:      make(chan wire.Message),
//		subscriptions: make(chan chan wire.Message),
//		quit:          make(chan struct{}),
//		peerTimeout:   30 * time.Second,
//		//reqDuration: 30*time.Second,
//	}
//	results := make(chan *jobResult)
//	quit := make(chan struct{})
//
//	wk := NewWorker(peer, "test")
//
//	// Start worker.
//	done := make(chan struct{})
//	go func() {
//		defer close(done)
//		wk.Run(results, quit)
//	}()
//
//	// Wait for it to subscribe to peer messages.
//	var sub chan wire.Message
//	select {
//	case sub = <-peer.subscriptions:
//	case <-time.After(1 * time.Second):
//		return nil, fmt.Errorf("did not subscribe to msgs")
//	}
//	peer.responses = sub
//
//	return &testCtx{
//		nextJob:    wk.NewJob(),
//		jobResults: results,
//		peer:       peer,
//		workerDone: done,
//	}, nil
//}
//
//// TestWorkerIgnoreMsgs tests that the worker handles being given the response
//// to its query after first receiving some non-matching messages.
//func TestWorkerIgnoreMsgs(t *testing.T) {
//	t.Parallel()
//
//	ctx, err := startWorker()
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	// Create a new task and give it to the worker.
//	task := makeJob()
//
//	select {
//	case ctx.nextJob <- task:
//	case <-time.After(1 * time.Second):
//		t.Fatalf("did not pick up job")
//	}
//
//	// The request should be sent to the peer.
//	select {
//	case <-ctx.peer.requests:
//	case <-time.After(time.Second):
//		t.Fatalf("request not sent")
//	}
//
//	// First give the worker a few random responses. These will all be
//	// ignored.
//	for i := 0; i < 5; i++ {
//		select {
//		case ctx.peer.responses <- &wire.MsgTx{}:
//		case <-time.After(time.Second):
//			t.Fatalf("resp not received")
//		}
//	}
//
//	// Answer the query with the correct response.
//
//	//
//	select {
//	case ctx.peer.responses <- finalResp:
//	case <-time.After(time.Second):
//		t.Fatalf("resp not received")
//	}
//
//	// The worker should respond with a job finished.
//	var result *jobResult
//	select {
//	case result = <-ctx.jobResults:
//	case <-time.After(time.Second):
//		t.Fatalf("response not received")
//	}
//
//	if result.err != nil {
//		t.Fatalf("response error: %v", result.err)
//	}
//
//	// Make sure the QueryJob instance in the result is different from the initial one
//	// supplied to the worker
//	if result.job == *task {
//		t.Fatalf("result's job should be different from the task's ")
//	}
//
//	// Make sure we are receiving the corresponding result for the given task.
//	if result.job.Index() != task.Index() {
//		t.Fatalf("result's job index should not be different from task's")
//	}
//
//	//Make sure job does not return as unfinished.
//	if result.unFinished {
//		t.Fatalf("got unfinished job")
//	}
//
//	// And the correct peer.
//	if result.peer != ctx.peer {
//		t.Fatalf("expected peer to be %v, was %v",
//			ctx.peer.Addr(), result.peer)
//	}
//}
//
//// TestWorkerTimeout tests that the worker will eventually return a Timeout
//// error if the query is not answered before the time limit.
//func TestWorkerTimeout(t *testing.T) {
//	t.Parallel()
//
//	const timeout = 50 * time.Millisecond
//
//	ctx, err := startWorker()
//	ctx.peer.peerTimeout = timeout
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	// Create a task with a small timeout.
//	task := makeJob()
//
//	// Give the worker the new job.
//	select {
//	case ctx.nextJob <- task:
//	case <-time.After(1 * time.Second):
//		t.Fatalf("did not pick up job")
//	}
//
//	// The request should be given to the peer.
//	select {
//	case <-ctx.peer.requests:
//	case <-time.After(time.Second):
//		t.Fatalf("request not sent")
//	}
//
//	// Don't anwer the query. This should trigger a timeout, and the worker
//	// should respond with an error result.
//	var result *jobResult
//	select {
//	case result = <-ctx.jobResults:
//	case <-time.After(time.Second):
//		t.Fatalf("response not received")
//	}
//
//	// Result should have a timeout error
//	if result.err != ErrQueryTimeout {
//		t.Fatalf("expected timeout, got: %v", result.err)
//	}
//
//	// Make sure the QueryJob instance in the result is different from the initial one
//	// supplied to the worker
//	if result.job == *task {
//		t.Fatalf("result's job should be different from the task's")
//	}
//
//	// Make sure we are receiving the corresponding result for the given task.
//	if result.job.Index() != task.Index() {
//		t.Fatalf("result's job index should not be different from task's")
//	}
//
//	// And the correct peer.
//	if result.peer != ctx.peer {
//		t.Fatalf("expected peer to be %v, was %v",
//			ctx.peer.Addr(), result.peer)
//	}
//
//	//Make sure job does not return as unfinished.
//	if result.unFinished {
//		t.Fatalf("got unfinished job")
//	}
//
//	// The worker should still wait for a feedback
//	// and so should not be able to pick up task
//	select {
//	case ctx.nextJob <- task:
//		t.Fatalf("worker still in feedback loop picked up job")
//	default:
//	}
//
//	// The worker should still be waiting for feedback
//	// and so should be able to pick up a response
//	select {
//	case ctx.peer.responses <- finalResp:
//	case <-time.After(time.Second):
//		t.Fatalf("worker should still be in feedback loop and should pick up response")
//	}
//}
//
//// TestWorkerDisconnect tests that the worker will return an error if the peer
//// disconnects, and that the worker itself is then shut down.
//// TODO(Maureen): Write tests for handling peer disconnect while handing job to peer
//func TestWorkerDisconnect(t *testing.T) {
//	t.Parallel()
//
//	ctx, err := startWorker()
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	// Give the worker a new job.
//	task := makeJob()
//	select {
//	case ctx.nextJob <- task:
//	case <-time.After(1 * time.Second):
//		t.Fatalf("did not pick up job")
//	}
//
//	// The request should be given to the peer.
//	select {
//	case <-ctx.peer.requests:
//	case <-time.After(time.Second):
//		t.Fatalf("request not sent")
//	}
//
//	// Disconnect the peer.
//	close(ctx.peer.quit)
//
//	// The worker should respond with a job failure.
//	var result *jobResult
//	select {
//	case result = <-ctx.jobResults:
//	case <-time.After(time.Second):
//		t.Fatalf("response not received")
//	}
//
//	if result.err != ErrPeerDisconnected {
//		t.Fatalf("expected peer disconnect, got: %v", result.err)
//	}
//
//	// Make sure the QueryJob instance in the result is different from the initial one
//	// supplied to the worker
//	if result.job == *task {
//		t.Fatalf("result's job should be different from task's")
//	}
//
//	// Make sure we are receiving the corresponding result for the given task.
//	if result.job.Index() != task.Index() {
//		t.Fatalf("result's job index should not be different from task's")
//	}
//
//	// And the correct peer.
//	if result.peer != ctx.peer {
//		t.Fatalf("expected peer to be %v, was %v",
//			ctx.peer.Addr(), result.peer)
//	}
//
//	//Make sure job does not return as unfinished.
//	if result.unFinished {
//		t.Fatalf("got unfinished job")
//	}
//
//	// No more jobs should be accepted by the worker after it has exited.
//	select {
//	case ctx.nextJob <- task:
//		t.Fatalf("exited worker did pick up job")
//	default:
//	}
//
//	// Finally, make sure the worker go routine exits.
//	select {
//	case <-ctx.workerDone:
//	case <-time.After(time.Second):
//		t.Fatalf("worker did not exit")
//	}
//}
//
//// TestWorkerProgress tests that the worker would return a result with the
//// Unfinished boolean set to true if job has progressed and not finished
//// as well as return a result with the Unfinished boolean set to false if job is finished.
//func TestWorkerProgress(t *testing.T) {
//	t.Parallel()
//
//	ctx, err := startWorker()
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	// We'll do two checks: first for a result that has progressed but not finished and
//	// then for a result that has finished
//	resp := progressResp
//	for i := 0; i < 2; i++ {
//		// Create a task.
//		task := makeJob()
//
//		select {
//		case ctx.nextJob <- task:
//		case <-time.After(1 * time.Second):
//			t.Fatalf("did not pick up job")
//		}
//
//		// The request should be given to the peer.
//		select {
//		case <-ctx.peer.requests:
//		case <-time.After(time.Second):
//			t.Fatalf("request not sent")
//		}
//
//		// Send a response.
//		select {
//		case ctx.peer.responses <- resp:
//		case <-time.After(time.Second):
//			t.Fatalf("resp not received")
//		}
//
//		// The worker should respond with a result that has the Unfinished boolean
//		// set to True.
//		var result *jobResult
//		select {
//		case result = <-ctx.jobResults:
//		case <-time.After(time.Second):
//			t.Fatalf("response not received")
//		}
//
//		switch resp {
//
//		case progressResp:
//			// Unfinished boolean should be set to true if response has progressed
//			// but not finished.
//			if !result.unFinished {
//				t.Fatalf("Unfinished boolean not set to true")
//			}
//			resp = finalResp
//		case finalResp:
//			// Unfinished boolean should be set to false if response has finished
//			if result.unFinished {
//				t.Fatalf("Unfinished boolean not set to true")
//			}
//		}
//
//		// No error should be returned
//		if result.err != nil {
//			t.Fatalf("expected no error, got: %v", result.err)
//		}
//
//		// Make sure the QueryJob instance in the result is different from the initial one
//		// supplied to the worker
//		if result.job == *task {
//			t.Fatalf("result's job should be different from task's")
//		}
//
//		// Make sure we are receiving the corresponding result for the given task.
//		if result.job.Index() != task.Index() {
//			t.Fatalf("result's job index should not be different from task's.\n"+
//				"Result index:%v \n Job index:%v \n", result.job.Index(), task.Index())
//		}
//
//		// And the correct peer.
//		if result.peer != ctx.peer {
//			t.Fatalf("expected peer to be %v, was %v",
//				ctx.peer.Addr(), result.peer)
//		}
//
//	}
//
//}
//
//// TestWorkerJobCanceled tests that the worker will return an error if the job is
//// canceled while the worker is handling it.
//func TestWorkerJobCanceled(t *testing.T) {
//	t.Parallel()
//
//	ctx, err := startWorker()
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	cancelChan := make(chan struct{})
//
//	// Give the worker a new job.
//	task := makeJob()
//	task.cancelChan = cancelChan
//
//	// We'll do two checks: first cancelling the job after it has been
//	// given to the peer, then handing the already canceled job to the
//	// worker.
//	canceled := false
//	for i := 0; i < 2; i++ {
//		select {
//		case ctx.nextJob <- task:
//		case <-time.After(1 * time.Second):
//			t.Fatalf("did not pick up job")
//		}
//
//		// If the request was not already canceled, it should be given
//		// to the peer.
//		if !canceled {
//			select {
//			case <-ctx.peer.requests:
//			case <-time.After(time.Second):
//				t.Fatalf("request not sent")
//			}
//
//			// Cancel the job.
//			close(cancelChan)
//			canceled = true
//		} else {
//			// If it was already canceled, it should not be given
//			// to the peer.
//			select {
//			case <-ctx.peer.requests:
//				t.Fatalf("canceled job was given to peer")
//			case <-time.After(10 * time.Millisecond):
//			}
//		}
//
//		// The worker should respond with a job failure.
//		var result *jobResult
//		select {
//		case result = <-ctx.jobResults:
//		case <-time.After(time.Second):
//			t.Fatalf("response not received")
//		}
//
//		if result.err != ErrJobCanceled {
//			t.Fatalf("expected job canceled, got: %v", result.err)
//		}
//
//		//Make sure job does not return as unfinished.
//		if result.unFinished {
//			t.Fatalf("got unfinished job")
//		}
//
//		// Make sure the QueryJob instance in the result is different from the initial one
//		// supplied to the worker
//		if result.job == *task {
//			t.Fatalf("result's job should be different from the task's")
//		}
//
//		// Make sure we are receiving the corresponding result for the given task.
//		if result.job.Index() != task.Index() {
//			t.Fatalf("result's job index should not be different from task's")
//		}
//
//		// And the correct peer.
//		if result.peer != ctx.peer {
//			t.Fatalf("expected peer to be %v, was %v",
//				ctx.peer.Addr(), result.peer)
//		}
//	}
//}
//
//// TestWorkerFeedbackErr will test if the result would return an error
//// that would be handled by the worker if there is an error returned while
//// sending a query.
//func TestWorkerFeedbackErr(t *testing.T) {
//	t.Parallel()
//
//	ErrAnyOtherError := errors.New("any other error received while sending query")
//	ctx, err := startWorker()
//	if err != nil {
//		t.Fatalf("unable to start worker: %v", err)
//	}
//
//	cancelChan := make(chan struct{})
//
//	// Give the worker a new job.
//	task := makeJob()
//	task.cancelChan = cancelChan
//
//	// We'll do three checks:
//	//1. Return an ErrResponseExistForQuery error which should return no error in result.
//	//2. Return an ErrAnyOtherError error which should return that same error in the result.
//	//3. Return no error which should return that same error in the result.
//	ctx.peer.err = ErrResponseExistForQuery
//	for i := 0; i < 3; i++ {
//
//		select {
//		case ctx.nextJob <- task:
//		case <-time.After(1 * time.Second):
//			t.Fatalf("did not pick up job")
//		}
//
//		select {
//		case <-ctx.peer.requests:
//			if ctx.peer.err != nil {
//				t.Fatalf("request sent when query failed")
//			}
//		case <-time.After(time.Second):
//			if ctx.peer.err == nil {
//				t.Fatalf("request not sent")
//			}
//		}
//
//		if ctx.peer.err == nil {
//
//			// Send a response.
//			select {
//			case ctx.peer.responses <- finalResp:
//			case <-time.After(time.Second):
//				t.Fatalf("resp not received")
//			}
//		}
//
//		// The worker should respond with a job failure.
//		var result *jobResult
//		select {
//		case result = <-ctx.jobResults:
//		case <-time.After(time.Second):
//			t.Fatalf("response not received")
//		}
//
//		switch ctx.peer.err {
//
//		case ErrResponseExistForQuery:
//
//			if result.err != nil {
//				t.Fatalf("response with error ErrResponseExistForQuery " +
//					"should not return any error in the result")
//			}
//			ctx.peer.err = ErrAnyOtherError
//		default:
//			// Unfinished boolean should be set to false if response has finished
//			if result.err != ctx.peer.err {
//				t.Fatalf("expected result's error to be %v, was %v",
//					ctx.peer.err, result.err)
//			}
//			ctx.peer.err = nil
//
//		}
//
//		//Make sure job does not return as unfinished.
//		if result.unFinished {
//			t.Fatalf("got unfinished job")
//		}
//
//		// Make sure the QueryJob instance in the result is different from the initial one
//		// supplied to the worker
//		if result.job == *task {
//			t.Fatalf("result's job should be different from the task's")
//		}
//
//		// Make sure we are receiving the corresponding result for the given task.
//		if result.job.Index() != task.Index() {
//			t.Fatalf("result's job index should not be different from task's")
//		}
//
//		// And the correct peer.
//		if result.peer != ctx.peer {
//			t.Fatalf("expected peer to be %v, was %v",
//				ctx.peer.Addr(), result.peer)
//		}
//	}
//}
//
////TODO(Maureen): Test of change in peer timeout
