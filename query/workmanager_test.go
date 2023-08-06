package query

import (
	"container/heap"
	"fmt"
	"github.com/btcsuite/btcd/wire"
	"sort"
	"strconv"
	"testing"
	"time"
)

type mockWorker struct {
	peer    BlkHdrPeer
	nextJob chan *QueryJob
	results chan *jobResult
}

var _ Worker = (*mockWorker)(nil)

func (m *mockWorker) NewJob() chan<- *QueryJob {
	return m.nextJob
}

func (m *mockWorker) Peer() BlkHdrPeer {
	return m.peer
}

func (m *mockWorker) Run(results chan<- *jobResult,
	quit <-chan struct{}) {

	// We'll forward the mocked responses on the result channel.
	for {
		select {
		case r := <-m.results:
			// Set the peer before forwarding it.
			r.peer = m.peer

			results <- r
		case <-quit:
			return
		}
	}
}

func TestNewWorkManager(t *testing.T) {

	cfg := &Config{}
	wm := New(cfg)
	if wm.cfg != cfg {
		t.Fatalf("workmanager config should be same as config")
	}
}

func TestWorkManagerStart(t *testing.T) {

	cfg := &Config{}
	wm := New(cfg)

	heap.Push(wm.work, &QueryJob{})

}

type mockCtx struct {
	wmgr     *WorkManager
	numWker  int
	peerChan chan BlkHdrPeer
}

func setup(numWorkers int, numWork int) mockCtx {

	peerChan := make(chan BlkHdrPeer)
	wm := New(&Config{
		ConnectedPeers: func() (<-chan BlkHdrPeer, func(), error) {
			return peerChan, func() {}, nil
		},
		Order: OrderPeers,
		IsEligibleWorkerFunc: func(r *activeWorker, next *QueryJob) bool {
			if r.activeJob != nil {

				return false
			}
			return true
		},
		Temp:      "test",
		NewWorker: NewWorker,
	})
	ctx := mockCtx{
		wmgr:     wm,
		peerChan: peerChan,
	}
	addWorkers(numWorkers, &ctx, true)

	heap.Init(wm.work)

	addWork(numWork, &ctx)

	return ctx
}

func addWork(num int, ctx *mockCtx) {

	for i := 0; i < num; i++ {
		w := &QueryJob{
			index: float64(i),
		}
		ctx.wmgr.workRWMtx.Lock()
		heap.Push(ctx.wmgr.work, w)
		ctx.wmgr.workRWMtx.Unlock()
	}
}

func addWorkers(num int, ctx *mockCtx, free bool) {

	init := ctx.numWker
	for i := init + 1; i <= num+init; i++ {
		peer := &mockPeer{
			reqDuration: time.Duration(i),
			addr:        fmt.Sprintf("%v", i),
		}
		wkr := mockWorker{
			peer:    peer,
			nextJob: make(chan *QueryJob),
		}
		onExit := make(chan struct{})
		fmt.Println("Adding work")
		if free {
			ctx.wmgr.workersRWMutex.Lock()
			ctx.wmgr.workers[peer.Addr()] = &activeWorker{
				w:         &wkr,
				activeJob: nil,
				onExit:    onExit,
			}
			ctx.wmgr.workersRWMutex.Unlock()
		} else {
			ctx.wmgr.workersRWMutex.Lock()
			ctx.wmgr.workers[peer.Addr()] = &activeWorker{
				w:         &wkr,
				activeJob: &QueryJob{},
				onExit:    onExit,
			}
			ctx.wmgr.workersRWMutex.Unlock()
		}
		fmt.Println("Out of adding work")
		ctx.numWker = i
	}

}

func createRequests(num int) []*Request {

	var r []*Request
	for i := 1; i <= num; i++ {
		r = append(r, &Request{
			Req: &wire.MsgTx{},
		})
	}
	return r
}

func TestDistributeWorkFuncWorkOrder(t *testing.T) {

	ctx := setup(0, 0)

	ctx.wmgr.cfg.IsEligibleWorkerFunc = func(r *activeWorker, next *QueryJob) bool {
		return true

	}

	addWorkers(1, &ctx, true)
	addWork(5, &ctx)

	if len(ctx.wmgr.workers) != 1 {
		t.Fatalf("Failed to add required number of peers for test")
	}

	if ctx.wmgr.work.Len() != 5 {
		t.Fatalf("Failed to add required number of tasks for test")
	}
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	r, ok := ctx.wmgr.workers[strconv.Itoa(1)]
	if !ok {
		t.Fatal("Failed to add workers")
	}

	workLen := ctx.wmgr.work.Len()
	for y := 0; y < workLen; y++ {

		w := r.w.(*mockWorker)
		fmt.Println("Checking....")
		select {
		case job := <-w.nextJob:
			fmt.Printf("in next job(%v) to peer %v, %v\n", ctx.wmgr.work.Len(), job.index, y)
			if int(job.index) != y {
				t.Fatalf("Work distribution out of order.\nExpected: %v.\nGotten: %v\n", y, int(job.index))
			}
		case <-time.After(time.Second):
			t.Fatalf("worker should pick up job \n")

		}
	}

	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("All work should be dispatched.\nExpected: 0.\nGotten: %v\n", ctx.wmgr.work.Len())
	}

}

func TestDistributeWorkFuncDispatchEligibleWorker(t *testing.T) {

	ctx := setup(4, 2)

	ctx.wmgr.cfg.IsEligibleWorkerFunc = func(r *activeWorker, next *QueryJob) bool {
		fmt.Println("In func eligible")
		if r.activeJob != nil {

			return false
		}

		if r.w.Peer().LastReqDuration()%2 != 0 {

			fmt.Printf("%v return true\n", r.w.Peer().Addr())
			return true
		}
		fmt.Printf("%v return false\n", r.w.Peer().Addr())
		return false

	}

	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	for y := 1; y <= 4; y++ {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			t.Error("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
			if y%2 == 0 {
				t.Fatalf("worker(%v) expected not to be eligible but picked up job \n", w.Peer().Addr())
			}
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
			if y%2 != 0 {
				t.Fatalf("worker(%v) expected to be eligible, should pick up job \n", w.Peer().Addr())
			}
		}
	}

	//Wait to pop work from heap before checking length.
	time.Sleep(2)
	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Work length expected to be zero after eligibility function test but got %v", ctx.wmgr.work.Len())
	}

	close(ctx.wmgr.quit)

	addWorkers(2, &ctx, true)

	addWork(2, &ctx)

	fmt.Printf("Test length workers %v\n", len(ctx.wmgr.workers))
	fmt.Printf("Test length work %v\n", ctx.wmgr.work.Len())

	for y := 4; y <= 6; y++ {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			fmt.Println(y)
			t.Fatal("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
			t.Fatalf("shutdown workmanager received job")
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
		}
	}

	ctx = setup(4, 2)

	ctx.wmgr.cfg.IsEligibleWorkerFunc = func(r *activeWorker, next *QueryJob) bool {
		fmt.Println("In func eligggggggggible")
		if r.activeJob != nil {

			return false
		}

		if r.w.Peer().LastReqDuration()%2 != 0 {

			fmt.Printf("%v return true\n", r.w.Peer().Addr())
			return false
		}
		fmt.Printf("%v return false\n", r.w.Peer().Addr())
		return true

	}
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	for y := 1; y <= 4; y++ {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			fmt.Println(y)
			t.Error("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
			if y%2 != 0 {
				t.Fatalf("worker(%v) expected to be eligible, should pick up job \n", w.Peer().Addr())
			}
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
			if y%2 == 0 {
				t.Fatalf("worker(%v) expected not to be eligible but picked up job \n", w.Peer().Addr())
			}
		}
	}

	//Wait to pop work from heap before checking length.
	time.Sleep(2)
	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Work length expected to be zero after eligibility function test but got %v", ctx.wmgr.work.Len())
	}

}

func TestDistributeWorkFuncLengthWorkAndWorker(t *testing.T) {

	ctx := setup(2, 0)

	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	for y := 1; y <= 2; y++ {
		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			t.Error("worker not of type mockWorker")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			t.Fatalf("worker should not pick up job")
		case <-time.After(time.Second):

		}
	}

	for r := range ctx.wmgr.workers {

		ctx.wmgr.workersRWMutex.Lock()
		delete(ctx.wmgr.workers, r)
		ctx.wmgr.workersRWMutex.Unlock()
	}
	ctx.numWker = 0
	addWork(2, &ctx)
	initialNumWork := ctx.wmgr.work.Len()

	time.Sleep(5)

	finalNumWork := ctx.wmgr.work.Len()
	fmt.Printf("Initial-%v and Final-%v", initialNumWork, finalNumWork)
	if initialNumWork != finalNumWork {
		t.Fatalf("Work should not be consumed.")
	}

	addWorkers(2, &ctx, true)

	for y := 1; y <= 2; y++ {
		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			t.Fatalf("Failed to add worker")
		}
		w, ok := r.w.(*mockWorker)
		if !ok {
			t.Fatalf("worker not of type mockWorker")
		}

		fmt.Printf("checking next job to peer (%v)(%v) %v\n", len(ctx.wmgr.workers), ctx.wmgr.work.Len(), w.Peer().LastReqDuration())
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
			t.Fatalf("worker(%v) should pick up job\n", w.Peer().Addr())

		}

	}
	initialNumWork = finalNumWork
	finalNumWork = ctx.wmgr.work.Len()
	if initialNumWork == finalNumWork {
		t.Fatalf("Work should be consumed.")
	}

}

func TestDistributeWorkFuncWorkerExit(t *testing.T) {

	ctx := setup(3, 2)

	initialNumWork := ctx.wmgr.work.Len()
	initialNumWker := len(ctx.wmgr.workers)
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	// choose a worker between 1 and 2
	r, ok := ctx.wmgr.workers[strconv.Itoa(2)]
	if !ok {
		t.Fatal("Failed to add workers")
	}
	close(r.onExit)
	for y := 1; y <= 3; y++ {
		fmt.Println(y)
		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			if y != 2 {
				t.Fatal("Failed to add workers")
			}
			continue
		}
		w := r.w.(*mockWorker)
		fmt.Println("Checking....")
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
		case <-time.After(time.Second):
			if y != 2 {
				t.Fatalf("worker(%v) should pick up job \n", w.Peer().Addr())
			}
		}
	}

	finalNumWork := ctx.wmgr.work.Len()
	if initialNumWork == finalNumWork {
		t.Fatalf("All work should be consumed.")
	}

	finalNumWker := len(ctx.wmgr.workers)

	if initialNumWker-1 != finalNumWker {
		fmt.Printf("final: %v.Initial: %v\n", finalNumWker, initialNumWker)
		t.Fatalf("Worker length should reduce by 1")
	}
}

func TestDistributeWorkFuncWorkerOrder(t *testing.T) {

	ctx := setup(3, 3)

	ctx.wmgr.cfg.Order = func(peers []BlkHdrPeer) {

		sort.Slice(peers, func(i, j int) bool {

			return peers[i].LastReqDuration() > peers[j].LastReqDuration()
		})
	}

	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()

	for y := 3; y >= 1; y-- {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			t.Error("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
			t.Fatalf("worker(%v) not in  expected order \n", w.Peer().Addr())

		}
	}

	//Wait to pop work from heap before checking length.
	time.Sleep(2)
	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Work length expected to be zero after eligibility function test but got %v", ctx.wmgr.work.Len())
	}

	close(ctx.wmgr.quit)

	addWorkers(2, &ctx, true)

	addWork(2, &ctx)

	fmt.Printf("Test length workers %v\n", len(ctx.wmgr.workers))
	fmt.Printf("Test length work %v\n", ctx.wmgr.work.Len())

	for y := 4; y >= 2; y-- {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			fmt.Println(y)
			t.Fatal("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())
			t.Fatalf("shutdown workmanager received job")
		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
		}
	}

	ctx = setup(6, 6)

	ctx.wmgr.cfg.Order = func(peers []BlkHdrPeer) {

		sort.Slice(peers, func(i, j int) bool {
			if peers[i].LastReqDuration()%2 == 0 && peers[j].LastReqDuration()%2 != 0 {
				return true
			} else if peers[i].LastReqDuration()%2 != 0 && peers[j].LastReqDuration()%2 == 0 {
				return false
			}
			// For equal or same parity elements, use regular comparison
			return peers[i].LastReqDuration() < peers[j].LastReqDuration()

		})
		for _, r := range peers {
			fmt.Println(r.LastReqDuration())
		}
	}

	var expectedOrder []int
	for y := 1; y <= 6; y++ {
		expectedOrder = append(expectedOrder, y)
	}

	sort.Slice(expectedOrder, func(i, j int) bool {
		if expectedOrder[i]%2 == 0 && expectedOrder[j]%2 != 0 {
			return true
		} else if expectedOrder[i]%2 != 0 && expectedOrder[j]%2 == 0 {
			return false
		}
		// For equal or same parity elements, use regular comparison
		return expectedOrder[i] < expectedOrder[j]

	})
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.distributeWorkFunc(ctx.wmgr.cfg.IsEligibleWorkerFunc)()
	fmt.Printf("%v", expectedOrder)
	for _, y := range expectedOrder {

		r, ok := ctx.wmgr.workers[strconv.Itoa(y)]
		if !ok {
			fmt.Println(y)
			t.Error("Failed to add workers")
		}
		w := r.w.(*mockWorker)
		select {
		case <-w.nextJob:
			fmt.Printf("in next job to peer %v\n", w.Peer().Addr())

		case <-time.After(time.Second):
			fmt.Printf("in timeout to peer %v\n", w.Peer().Addr())
			t.Fatalf("worker(%v) out of expected order \n", w.Peer().Addr())
		}
	}

	//Wait to pop work from heap before checking length.
	time.Sleep(2)
	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Work length expected to be zero after test but got %v", ctx.wmgr.work.Len())
	}
}

func TestWorkDispatcherPeersConnected(t *testing.T) {
	ctx := setup(0, 0)
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.workDispatcher()

	for i := 1; i <= 5; i++ {
		select {
		case ctx.peerChan <- &mockPeer{
			reqDuration:   time.Duration(i),
			addr:          fmt.Sprintf("%v", i),
			subscriptions: make(chan chan wire.Message),
		}:

		case <-time.After(time.Second):
			t.Fatalf("work dispatcher failed to receive peer\n")
		}
	}
	time.Sleep(2)
	if len(ctx.wmgr.workers) != 5 {
		t.Fatalf("Number of workers gotten: %v, expected amount: 5", len(ctx.wmgr.workers))
	}

	for i := 1; i <= 5; i++ {
		_, ok := ctx.wmgr.workers[strconv.Itoa(i)]
		if !ok {
			t.Fatalf("worker (%v) not created", i)
		}

	}
	//Change duration to detect duplicates
	for i := 1; i <= 5; i++ {
		select {
		case ctx.peerChan <- &mockPeer{
			reqDuration: time.Duration(i + 1),
			addr:        fmt.Sprintf("%v", i),
		}:

		case <-time.After(time.Second):
			t.Fatalf("work dispatcher failed to receive peer\n")
		}
	}

	if len(ctx.wmgr.workers) != 5 {
		t.Fatalf("Number of workers gotten: %v, expected amount: 5", len(ctx.wmgr.workers))
	}

	for i := 1; i <= 5; i++ {
		r, ok := ctx.wmgr.workers[strconv.Itoa(i)]
		if !ok {
			t.Fatalf("worker (%v) not created", i)
		}

		peer := r.w.Peer().(*mockPeer)
		if int(peer.reqDuration) != i {
			t.Fatalf("worker (%v) was overriden by duplicate", i)
		}
	}

	for i := 1; i <= 5; i++ {
		r, ok := ctx.wmgr.workers[strconv.Itoa(i)]
		if !ok {
			t.Fatalf("worker (%v) not created", i)
		}

		peer := r.w.Peer().(*mockPeer)
		fmt.Println(peer.subscriptions == nil)
		select {
		case <-peer.subscriptions:
		case <-time.After(time.Second):
			t.Fatalf("worker (%v) not running", i)
		}

	}

}

func TestWorkDispatcherResultTimeout(t *testing.T) {
	ctx := setup(0, 0)
	addWorkers(1, &ctx, false)
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.workDispatcher()

	select {
	case ctx.wmgr.newBatches <- &batch{
		requests: createRequests(1),
		options: &queryOptions{
			noTimeout: true,
		},
	}:
	case <-time.After(time.Second):
		t.Fatalf("batch not sent")
	}
	time.Sleep(1)

	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected, 1 but got %v", ctx.wmgr.work.Len())
	}

	if len(ctx.wmgr.workers) != 1 {
		t.Fatalf("Could not create all peers for test")
	}
	r, ok := ctx.wmgr.workers[strconv.Itoa(1)]

	if !ok {
		t.Fatalf("No worker found")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to have an active job")

	}
	job := ctx.wmgr.work.Peek().(*QueryJob)
	heap.Pop(ctx.wmgr.work)

	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Length of tasks expected, 0 but got %v", ctx.wmgr.work.Len())
	}
	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job:        *job,
		peer:       r.w.Peer(),
		err:        ErrQueryTimeout,
		unFinished: false,
	}:
	case <-time.After(time.Second):
		t.Fatalf("result not received")
	}

	time.Sleep(5)
	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected, 1 but got %v", ctx.wmgr.work.Len())
	}

	job = ctx.wmgr.work.Peek().(*QueryJob)

	intIndex := int64(job.Index())
	test := job.Index() - 0.0005

	if float64(intIndex) == test {
		t.Fatalf("New job should not be created")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to still have an active job")

	}
}

func TestWorkDispatcherResultUnfinishedTasksAndJobIndexDuplicate(t *testing.T) {
	ctx := setup(0, 0)
	addWorkers(2, &ctx, false)
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.workDispatcher()

	select {
	case ctx.wmgr.newBatches <- &batch{
		requests: createRequests(2),
		options: &queryOptions{
			noTimeout: true,
		},
	}:
	case <-time.After(time.Second):
		t.Fatalf("batch not sent")
	}
	time.Sleep(3)
	if ctx.wmgr.work.Len() != 2 {
		t.Fatalf("Length of tasks expected, 2 but got %v", ctx.wmgr.work.Len())
	}

	if len(ctx.wmgr.workers) != 2 {
		t.Fatalf("Could not create all peers for test")
	}
	r, ok := ctx.wmgr.workers[strconv.Itoa(1)]

	if !ok {
		t.Fatalf("No worker found")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to have an active job")

	}

	job := ctx.wmgr.work.Peek().(*QueryJob)
	initialJob := job
	heap.Pop(ctx.wmgr.work)

	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected, 1 but got %v", ctx.wmgr.work.Len())
	}
	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job:        *job,
		peer:       r.w.Peer(),
		err:        nil,
		unFinished: true,
	}:
	case <-time.After(time.Second * 6):
		t.Fatalf("result not received")
	}

	time.Sleep(5)
	if ctx.wmgr.work.Len() != 2 {
		t.Fatalf("work dispatcher did not retry expected number of task.\n"+
			"Expected: 1\nGotten: %v\n", ctx.wmgr.work.Len())
	}

	job = ctx.wmgr.work.Peek().(*QueryJob)

	intIndex := int64(job.Index())
	test := job.Index() - 0.0005

	if float64(intIndex) != test {
		t.Fatalf("Expected new index in multiples of 0.0005 to indicate new job create from old one")
	}

	if r.activeJob != nil {
		t.Fatalf("Expected worker to not have active job")

	}

	//Send result with same job index
	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job: QueryJob{
			index: initialJob.index,
		},
		peer:       r.w.Peer(),
		err:        nil,
		unFinished: true,
	}:
	case <-time.After(time.Second * 6):
		t.Fatalf("result not received")
	}

	time.Sleep(5)
	if ctx.wmgr.work.Len() != 2 {
		t.Fatalf("work dispatcher did not retry expected number of task.\n"+
			"Expected: 1\nGotten: %v\n", ctx.wmgr.work.Len())
	}
	//
	//
	//
	//
	//

	//	Sending result with  a different job index included in the batch

	r, ok = ctx.wmgr.workers[strconv.Itoa(2)]

	if !ok {
		t.Fatalf("No worker found")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to have an active job")

	}

	// pop first task in heap which according to the sorting order should be the task
	// created by the previous unfinished task.
	heap.Pop(ctx.wmgr.work)
	job = ctx.wmgr.work.Peek().(*QueryJob)

	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected, 1 but got %v", ctx.wmgr.work.Len())
	}

	heap.Pop(ctx.wmgr.work)
	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Length of tasks expected, 0 but got %v", ctx.wmgr.work.Len())
	}
	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job:        *job,
		peer:       r.w.Peer(),
		err:        nil,
		unFinished: true,
	}:
	case <-time.After(time.Second * 6):
		t.Fatalf("result not received")
	}

	time.Sleep(5)
	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("work dispatcher did not retry expected number of task.\n"+
			"Expected: 1\nGotten: %v\n", ctx.wmgr.work.Len())
	}

	job = ctx.wmgr.work.Peek().(*QueryJob)

	intIndex = int64(job.Index())
	test = job.Index() - 0.0005

	if float64(intIndex) != test {
		t.Fatalf("Expected new index in multiples of 0.0005 to indicate new job create from old one")
	}

	if r.activeJob != nil {
		t.Fatalf("Expected worker to not have active job")

	}

}

func TestWorkDispatcherResultJobCancelled(t *testing.T) {
	ctx := setup(0, 0)
	addWorkers(1, &ctx, false)

	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.workDispatcher()

	errChan := make(chan error)
	select {
	case ctx.wmgr.newBatches <- &batch{
		requests: createRequests(1),
		options: &queryOptions{
			errChan: errChan,
		},
	}:
	case <-time.After(time.Second):
		t.Fatalf("batch not sent")
	}

	time.Sleep(2)
	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected,  1but got %v", ctx.wmgr.work.Len())
	}
	if len(ctx.wmgr.workers) != 1 {
		t.Fatalf("Could not create required number of peers for test")
	}
	r, ok := ctx.wmgr.workers[strconv.Itoa(1)]

	if !ok {
		t.Fatalf("No worker found")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to have an active job")

	}

	job := ctx.wmgr.work.Peek().(*QueryJob)
	heap.Pop(ctx.wmgr.work)

	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job:        *job,
		peer:       r.w.Peer(),
		err:        ErrJobCanceled,
		unFinished: false,
	}:
	case <-time.After(time.Second):
		t.Fatalf("result (%v) not sent", job.index)
	}

	select {
	case err := <-errChan:
		if err != ErrJobCanceled {
			t.Fatalf("Got wrong error. Expected: %v but got %v", ErrJobCanceled, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Expected error")
	}

	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Length of tasks expected, 0 but got %v", ctx.wmgr.work.Len())

	}
}

func TestWorkDispatcherResultBatchTimeout(t *testing.T) {
	ctx := setup(0, 0)
	addWorkers(1, &ctx, false)

	errChan := make(chan error)
	ctx.wmgr.wg.Add(1)
	go ctx.wmgr.workDispatcher()

	select {
	case ctx.wmgr.newBatches <- &batch{
		requests: createRequests(1),
		options: &queryOptions{
			timeout: 2 * time.Second,
			errChan: errChan,
		},
	}:
	case <-time.After(time.Second):
		t.Fatalf("batch not sent")
	}
	time.Sleep(2)
	if ctx.wmgr.work.Len() != 1 {
		t.Fatalf("Length of tasks expected, 1 but got %v", ctx.wmgr.work.Len())
	}

	if len(ctx.wmgr.workers) != 1 {
		t.Fatalf("Length of workers expected, 1 but got %v", len(ctx.wmgr.workers))
	}
	r, ok := ctx.wmgr.workers[strconv.Itoa(1)]

	if !ok {
		t.Fatalf("No worker found")
	}

	if r.activeJob == nil {
		t.Fatalf("Expected worker to have an active job")

	}

	job := ctx.wmgr.work.Peek().(*QueryJob)
	heap.Pop(ctx.wmgr.work)

	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Length of tasks expected, 0 but got %v", ctx.wmgr.work.Len())
	}

	select {
	case ctx.wmgr.jobResults <- &jobResult{
		job:        *job,
		peer:       r.w.Peer(),
		err:        nil,
		unFinished: false,
	}:
	case <-time.After(time.Second):
		t.Fatalf("result (%v) not sent", job.index)
	}

	//Receive timeout err for batch
	select {
	case err := <-errChan:
		if err == ErrQueryTimeout {
			t.Fatalf("Got wrong error. Expected: %v but got %v", ErrQueryTimeout, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Expected error")
	}

	if r.activeJob != nil {
		t.Fatalf("Expected worker to have no active job")

	}

	if ctx.wmgr.work.Len() != 0 {
		t.Fatalf("Length of tasks expected, 0 but got %v", ctx.wmgr.work.Len())
	}

}

func TestWorkManangerQuery(t *testing.T) {

	ctx := setup(0, 0)

	go ctx.wmgr.Query(createRequests(5))

	defaultOption := defaultQueryOptions()

	select {
	case b := <-ctx.wmgr.newBatches:
		if len(b.requests) != 5 {

			t.Fatalf("Expected 5 batch requests but got %v", len(b.requests))
		}
		if b.options.timeout != defaultOption.timeout {

			t.Fatalf("Expected %v as timeout value but got %v", defaultOption.timeout, b.options.timeout)
		}
		if b.options.encoding != defaultOption.encoding {

			t.Fatalf("Expected %v as encoding value but got %v", defaultOption.encoding, b.options.encoding)
		}
		if b.options.cancelChan != defaultOption.cancelChan {

			t.Fatalf("Expected %v as cancelChan value but got %v", defaultOption.cancelChan, b.options.cancelChan)
		}

	case <-time.After(time.Second):
		t.Fatalf("Did not receive batch requests")
	}

	errChan := make(chan error)
	go ctx.wmgr.Query(createRequests(2), ErrChan(errChan))

	close(ctx.wmgr.quit)

	select {

	case err := <-errChan:
		if err != ErrWorkManagerShuttingDown {
			t.Fatalf("Got unexpected error: %v. Expected %v", err, ErrWorkManagerShuttingDown)
		}
	case <-time.After(time.Second):
		t.Fatalf("Expected to receive error")
	}

}

//Note: Cannot write test for receiving quit signal for distributeFunc as it turns out flaky
