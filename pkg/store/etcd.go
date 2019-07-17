package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"sync"
	"time"
)

func encodeJob(job *workerBuildJob) (string, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(job)
	return buffer.String(), err
}

func decodeJob(data string) (*workerBuildJob, error) {
	var buffer bytes.Buffer
	buffer.Write([]byte(data))
	dec := gob.NewDecoder(&buffer)
	var job workerBuildJob
	err := dec.Decode(&job)
	return &job, err
}

type EtcdJobStore struct {
	dedupeKeyFormat util.DigestKeyFormat
	jobClient       *clientv3.Client
	dedupeClient    *clientv3.Client
}

func NewEtcdJobStore(dedupeKeyFormat util.DigestKeyFormat, cluster []string) (JobStore, error) {
	jobClient, err := clientv3.New(clientv3.Config{Endpoints: cluster})
	if err != nil {
		return nil, err
	}

	jobClient.KV = namespace.NewKV(jobClient.KV, "job-store/")
	jobClient.Watcher = namespace.NewWatcher(jobClient.Watcher, "job-store/")
	jobClient.Lease = namespace.NewLease(jobClient.Lease, "job-store/")

	dedupeClient, err := clientv3.New(clientv3.Config{Endpoints: cluster})
	if err != nil {
		return nil, err
	}

	dedupeClient.KV = namespace.NewKV(jobClient.KV, "job-dedupe/")
	dedupeClient.Watcher = namespace.NewWatcher(jobClient.Watcher, "job-dedupe/")
	dedupeClient.Lease = namespace.NewLease(jobClient.Lease, "job-dedupe/")

	return &EtcdJobStore{dedupeKeyFormat, jobClient, dedupeClient}, nil
}

func (js EtcdJobStore) Submit(ctx context.Context, executeRequest *remoteexecution.ExecuteRequest) (*workerBuildJob, bool, error) {
	deduplicationKey := js.getDigest(executeRequest)
	resp, err := js.dedupeClient.Get(ctx, deduplicationKey)
	if err != nil {
		return nil, false, fmt.Errorf("error when dedupe key: %s", err)
	}

	var exists = false
	var job *workerBuildJob

	if resp.Count == 0 {
		job = &workerBuildJob{
			Name:             uuid.Must(uuid.NewRandom()).String(),
			ActionDigest:     executeRequest.ActionDigest,
			DeduplicationKey: deduplicationKey,
			ExecuteRequest:   *executeRequest,
			Timestamp:        time.Now(),
			Stage:            remoteexecution.ExecuteOperationMetadata_QUEUED,
		}

		err := js.Update(ctx, job)
		if err != nil {
			return nil, exists, fmt.Errorf("error when creating new job on submit: %s", err)
		}
	} else {
		resp, err := js.jobClient.Get(ctx, string(resp.Kvs[0].Value))
		if err != nil {
			return nil, exists, fmt.Errorf("error when getting existing submitted job: %s", err)
		}

		if resp.Count == 1 {
			exists = true
			job, err = decodeJob(string(resp.Kvs[0].Value))
			if err != nil {
				return nil, exists, fmt.Errorf("error when decoding job: %s", err)
			}
		} else {
			return nil, exists, fmt.Errorf("no job with dedup key %s exists", deduplicationKey)
		}
	}

	return job, exists, nil
}

func (js EtcdJobStore) getDigest(executeRequest *remoteexecution.ExecuteRequest) string {
	digest, err := util.NewDigest(executeRequest.InstanceName, executeRequest.ActionDigest)
	if err != nil {
	}
	deduplicationKey := digest.GetKey(js.dedupeKeyFormat)
	return deduplicationKey
}

func (js EtcdJobStore) Get(ctx context.Context, jobName string) (*workerBuildJob, error) {
	resp, err := js.jobClient.Get(ctx, jobName)

	if err == nil {
		switch resp.Count {
		case 0:
		case 1:
			return decodeJob(string(resp.Kvs[0].Value))
		default:
			err = fmt.Errorf("multiple jobs found with id %s", jobName)
		}
	}

	return nil, err
}

func (js EtcdJobStore) Delete(ctx context.Context, str string) error {
	_, err := js.jobClient.Delete(ctx, str)
	if err != nil {
		return err
	}

	return nil
}

func (js EtcdJobStore) Update(ctx context.Context, job *workerBuildJob) error {
	encJob, err := encodeJob(job)
	if err != nil {
		return err
	}

	_, err = js.jobClient.Put(ctx, job.Name, encJob)
	if err != nil {
		return fmt.Errorf("could not put job %v: error: %s", job, err)
	}

	return nil
}

type EtcdJobStateNotifier struct {
	watchChan clientv3.WatchChan
	conds     map[string]*sync.Cond
}

func NewEtcdJobStateNotifier(ctx context.Context, cluster []string) (JobStateNotifier, error) {
	jobClient, err := clientv3.New(clientv3.Config{Endpoints: cluster})
	if err != nil {
		return nil, err
	}

	watcher := jobClient.Watch(ctx, "job-store/", clientv3.WithPrefix(), clientv3.WithPrevKV())
	notifier := &EtcdJobStateNotifier{watcher, map[string]*sync.Cond{}}
	go notifier.watch(ctx)
	return notifier, nil
}

func (jn EtcdJobStateNotifier) watch(ctx context.Context) error {
	for update := range jn.watchChan {
		for _, event := range update.Events {
			if event.Type == mvccpb.PUT && event.PrevKv != nil {
				curr, err := decodeJob(string(event.Kv.Value))
				if err != nil {
					return err
				}

				prev, err := decodeJob(string(event.PrevKv.Value))
				if err != nil {
					return err
				}

				if prev.Stage != curr.Stage {
					val, ok := jn.conds[curr.Name]
					if ok {
						val.Broadcast()
						// TODO(arlyon): delete when complete
					}
				}
			}
		}
	}

	return nil
}

func (jn EtcdJobStateNotifier) Wait(ctx context.Context, jobName string) error {
	val, ok := jn.conds[jobName]
	if !ok {
		val = sync.NewCond(&sync.Mutex{})
		jn.conds[jobName] = val
	}

	val.L.Lock()
	val.Wait()
	val.L.Unlock()
	return nil
}

type EtcdJobPriorityQueue struct {
	queue *PriorityQueue
}

func NewEtcdJobPriorityQueue(cluster []string) (JobQueue, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: cluster})
	if err != nil {
		return nil, err
	}

	return &EtcdJobPriorityQueue{NewPriorityQueue(client, "job-queue")}, nil
}

func (jh *EtcdJobPriorityQueue) Push(ctx context.Context, job *workerBuildJob) error {
	var priority int32
	if job.ExecuteRequest.ExecutionPolicy != nil {
		priority = job.ExecuteRequest.ExecutionPolicy.Priority
	}
	if priority < 0 {
		return fmt.Errorf("cannot have negative priority")
	}

	return jh.queue.Enqueue(ctx, job.Name, uint16(priority))
}

func (jh *EtcdJobPriorityQueue) Pop(ctx context.Context) (*string, error) {
	jobName, err := jh.queue.Dequeue(ctx)
	return &jobName, err
}

type EtcdBotJobMap struct {
	client *clientv3.Client
}

func NewEtcdBotJobMap(cluster []string) (BotJobMap, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: cluster})
	if err != nil {
		return nil, err
	}

	client.KV = namespace.NewKV(client.KV, "bot-job/")
	client.Watcher = namespace.NewWatcher(client.Watcher, "bot-job/")
	client.Lease = namespace.NewLease(client.Lease, "bot-job/")

	return &EtcdBotJobMap{client: client}, nil
}

func (bjm EtcdBotJobMap) Put(ctx context.Context, bot string, job string) error {
	_, err := bjm.client.Put(ctx, bot, job)
	return err
}

func (bjm EtcdBotJobMap) Delete(ctx context.Context, bot string) error {
	_, err := bjm.client.Delete(ctx, bot)
	return err
}

func (bjm EtcdBotJobMap) Get(ctx context.Context, botId string) (*string, error) {
	resp, err := bjm.client.Get(ctx, botId)

	if err == nil {
		switch resp.Count {
		case 0:
		case 1:
			str := string(resp.Kvs[0].Value)
			return &str, err
		default:
			err = fmt.Errorf("multiple bots found with id %s", botId)
		}
	}

	return nil, err
}
