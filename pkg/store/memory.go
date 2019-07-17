package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
)

type MemoryJobStore struct {
	jobMap          map[string]*workerBuildJob
	dupJobMap       map[string]*workerBuildJob
	doneCondMap     map[string]*sync.Cond
	dedupeKeyFormat util.DigestKeyFormat

	jobsLock sync.Mutex
}

func NewMemoryJobStore(format util.DigestKeyFormat) (JobStore, JobStateNotifier) {
	store := &MemoryJobStore{
		jobMap:          map[string]*workerBuildJob{},
		dupJobMap:       map[string]*workerBuildJob{},
		doneCondMap:     map[string]*sync.Cond{},
		dedupeKeyFormat: format,
	}

	return store, store
}

func (js *MemoryJobStore) Submit(ctx context.Context, executeRequest *remoteexecution.ExecuteRequest) (*workerBuildJob, bool, error) {
	js.jobsLock.Lock()
	defer js.jobsLock.Unlock()
	deduplicationKey := js.getDigest(executeRequest)
	job, ok := js.dupJobMap[deduplicationKey]

	if !ok {
		job = &workerBuildJob{
			Name:             uuid.Must(uuid.NewRandom()).String(),
			ActionDigest:     executeRequest.ActionDigest,
			DeduplicationKey: deduplicationKey,
			ExecuteRequest:   *executeRequest,
			Timestamp:        time.Now(),
			Stage:            remoteexecution.ExecuteOperationMetadata_QUEUED,
		}
		js.jobMap[job.Name] = job
		js.dupJobMap[deduplicationKey] = job
		js.doneCondMap[job.Name] = sync.NewCond(&js.jobsLock)
		return job, false, nil
	}

	return job, true, nil
}

func (js *MemoryJobStore) getDigest(executeRequest *remoteexecution.ExecuteRequest) string {
	digest, err := util.NewDigest(executeRequest.InstanceName, executeRequest.ActionDigest)
	if err != nil {
	}
	deduplicationKey := digest.GetKey(js.dedupeKeyFormat)
	return deduplicationKey
}

func (js MemoryJobStore) Get(ctx context.Context, str string) (*workerBuildJob, error) {
	item, ok := js.jobMap[str]
	if !ok {
		return nil, nil
	}
	return item, nil
}

func (js *MemoryJobStore) Delete(ctx context.Context, str string) error {
	job, err := js.Get(ctx, str)
	if err != nil {
		return err
	} else if job == nil {
		return fmt.Errorf("called delete on non-existent job")
	}

	js.jobsLock.Lock()
	defer js.jobsLock.Unlock()
	delete(js.jobMap, str)
	delete(js.dupJobMap, job.DeduplicationKey)
	return nil
}

func (js *MemoryJobStore) Update(ctx context.Context, job *workerBuildJob) error {
	js.jobsLock.Lock()
	defer js.jobsLock.Unlock()
	js.dupJobMap[job.DeduplicationKey] = job
	js.jobMap[job.Name] = job

	if job.Stage == remoteexecution.ExecuteOperationMetadata_COMPLETED {
		js.doneCondMap[job.Name].Broadcast()
		delete(js.doneCondMap, job.Name)
	}
	return nil
}

func (js *MemoryJobStore) Wait(ctx context.Context, name string) error {
	js.jobsLock.Lock()
	defer js.jobsLock.Unlock()
	cond, ok := js.doneCondMap[name]
	if !ok {
		return nil
	}
	cond.Wait()
	return nil
}

// TODO(arlyon): convert a into a heap
type MemoryJobHeap struct {
	heap                       []QueueItem
	lock                       sync.Mutex
	jobsPendingInsertionWakeup *sync.Cond
}

type QueueItem struct {
	name     string
	priority int32
	time     time.Time
}

func NewMemoryJobHeap() JobQueue {
	mem := &MemoryJobHeap{}
	mem.jobsPendingInsertionWakeup = sync.NewCond(&mem.lock)
	return mem
}

func (jh *MemoryJobHeap) Len() int {
	return len(jh.heap)
}

// Less consults the priority of the two
// items at index i, and j, returning true
// if the priority of i is less than j or
// in the case that they are equal, returns insertion order.
func (jh *MemoryJobHeap) Less(i, j int) bool {
	jobI, jobJ := jh.heap[i], jh.heap[j]
	return jobI.priority > jobJ.priority || (jobI.priority == jobJ.priority && jobI.time.Before(jobJ.time))
}

func (jh *MemoryJobHeap) Swap(ctx context.Context, i, j int) error {
	l := jh.heap
	l[i], l[j] = l[j], l[i]
	return nil
}

func (jh *MemoryJobHeap) Push(ctx context.Context, job *workerBuildJob) error {
	jh.lock.Lock()
	defer jh.lock.Unlock()
	jh.jobsPendingInsertionWakeup.Broadcast()

	var priority int32
	if job.ExecuteRequest.ExecutionPolicy != nil {
		priority = job.ExecuteRequest.ExecutionPolicy.Priority
	}

	jh.heap = append(jh.heap, QueueItem{job.Name, priority, job.Timestamp})
	return nil
}

func (jh *MemoryJobHeap) Pop(ctx context.Context) (*string, error) {
	// TODO(arlyon): wait with context
	jh.lock.Lock()
	defer jh.lock.Unlock()
	for jh.Len() == 0 {
		jh.jobsPendingInsertionWakeup.Wait()
	}

	if err := ctx.Err(); err != nil {
		jh.jobsPendingInsertionWakeup.Signal()
		return nil, err
	}

	old := jh.heap
	n := len(old)
	jobName := old[n-1]
	jh.heap = old[0 : n-1]
	return &jobName.name, nil
}

type MemoryBotJobMap map[string]string

func (bjm MemoryBotJobMap) Put(ctx context.Context, bot string, job string) error {
	bjm[bot] = job
	return nil
}

func (bjm MemoryBotJobMap) Delete(ctx context.Context, bot string) error {
	delete(bjm, bot)
	return nil
}

func (bjm MemoryBotJobMap) Get(ctx context.Context, bot string) (*string, error) {
	item, ok := bjm[bot]
	if !ok {
		return nil, nil
	}
	return &item, nil
}
