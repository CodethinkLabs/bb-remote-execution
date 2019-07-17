package builder

import (
	"context"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	schedulerconfig "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	remoteworker "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-remote-execution/pkg/store"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
)

// our grpc servers for the scheduler and executor
type workerBuildQueue struct {
	jobStore    store.JobStore
	jobQueue    store.JobQueue
	jobNotifier store.JobStateNotifier
	botJobMap   store.BotJobMap
}

// NewWorkerBuildQueue creates an execution server that places execution
// requests in a queue. These execution requests may be extracted by
// workers.
func NewWorkerBuildQueue(
	deduplicationKeyFormat util.DigestKeyFormat,
	jobStoreConfig *schedulerconfig.SchedulerBackendConfiguration,
	jobQueueConfig *schedulerconfig.SchedulerBackendConfiguration,
	botJobMapConfig *schedulerconfig.SchedulerBackendConfiguration) (builder.BuildQueue, remoteworker.BotServer) {

	var bq workerBuildQueue

	// configure the job store
	switch jobStoreConfig.Backend.(type) {
	case *schedulerconfig.SchedulerBackendConfiguration_Memory:
		jobStore, jobNotifier := store.NewMemoryJobStore(deduplicationKeyFormat)
		bq.jobStore = jobStore
		bq.jobNotifier = jobNotifier
	case *schedulerconfig.SchedulerBackendConfiguration_Etcd:
		clusterAddress := jobStoreConfig.Backend.(*schedulerconfig.SchedulerBackendConfiguration_Etcd).Etcd.ClusterAddress

		jobStore, err := store.NewEtcdJobStore(deduplicationKeyFormat, clusterAddress)
		if err != nil {
			panic(err)
		}

		jobNotifier, err := store.NewEtcdJobStateNotifier(context.TODO(), clusterAddress)
		if err != nil {
			panic(err)
		}

		bq.jobStore = jobStore
		bq.jobNotifier = jobNotifier
	}

	// configure the job queue
	switch jobQueueConfig.Backend.(type) {
	case *schedulerconfig.SchedulerBackendConfiguration_Memory:
		bq.jobQueue = store.NewMemoryJobHeap()
	case *schedulerconfig.SchedulerBackendConfiguration_Etcd:
		clusterAddress := jobQueueConfig.Backend.(*schedulerconfig.SchedulerBackendConfiguration_Etcd).Etcd.ClusterAddress

		jobQueue, err := store.NewEtcdJobPriorityQueue(clusterAddress)
		if err != nil {
			panic(err)
		}

		bq.jobQueue = jobQueue
	}

	// configure the bot job map
	switch botJobMapConfig.Backend.(type) {
	case *schedulerconfig.SchedulerBackendConfiguration_Memory:
		bq.botJobMap = store.MemoryBotJobMap{}
	case *schedulerconfig.SchedulerBackendConfiguration_Etcd:
		clusterAddress := botJobMapConfig.Backend.(*schedulerconfig.SchedulerBackendConfiguration_Etcd).Etcd.ClusterAddress

		botJobMap, err := store.NewEtcdBotJobMap(clusterAddress)
		if err != nil {
			panic(err)
		}

		bq.botJobMap = botJobMap
	}

	return &bq, &bq
}

// TODO(arlyon): transaction and rollback in case of error
// currently if a step errors out, jobs may be lost.
func (bq *workerBuildQueue) getJob(ctx context.Context, botSession *remoteworker.BotSessionSend) (*remoteworker.BotSessionResponse, error) {
	jobName, err := bq.jobQueue.Pop(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "error while popping job from queue: "+err.Error())
	}

	err = bq.botJobMap.Put(ctx, botSession.BotId.String(), *jobName)
	if err != nil {
		return nil, status.Error(codes.Internal, "error while assigning bot to job: "+err.Error())
	}

	job, err := bq.jobStore.Get(ctx, *jobName)
	if err != nil {
		return nil, status.Error(codes.Internal, "error while getting bot job from store: "+err.Error())
	}

	job.Stage = remoteexecution.ExecuteOperationMetadata_EXECUTING
	err = bq.jobStore.Update(ctx, job)
	if err != nil {
		return nil, status.Error(codes.Internal, "error while setting job state to executing: "+err.Error())
	}

	botSessionResponse := &remoteworker.BotSessionResponse{
		Execute: &remoteworker.BotSessionResponse_Request{
			Request: &job.ExecuteRequest,
		},
	}

	return botSessionResponse, nil
}

func (bq *workerBuildQueue) Update(ctx context.Context, botSession *remoteworker.BotSessionSend) (*remoteworker.BotSessionResponse, error) {
	botId := botSession.BotId.String()

	switch botSession.Execute.(type) {
	case *remoteworker.BotSessionSend_None:
		jobName, err := bq.botJobMap.Get(ctx, botId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		} else if jobName == nil {
			return bq.getJob(ctx, botSession)
		}

		// TODO(arlyon) transaction and rollback in case of error
		// error: bot doesn't know about it's job
		job, err := bq.jobStore.Get(ctx, *jobName)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if job != nil {
			err = bq.botJobMap.Delete(ctx, botId)
			if err != nil {
				return nil, status.Errorf(codes.Internal, err.Error())
			}

			// complete the job, and propagate the error up to client
			job.Stage = remoteexecution.ExecuteOperationMetadata_COMPLETED
			job.ExecuteResponse = &remoteexecution.ExecuteResponse{
				Status: status.New(codes.Internal, "missing job on bot "+botId).Proto(),
			}

			err = bq.jobStore.Update(ctx, job)
			if err != nil {
				return nil, status.Errorf(codes.Internal, err.Error())
			}
		}

		return nil, status.Errorf(codes.Internal, "missing job on bot %s", botId)

	case *remoteworker.BotSessionSend_Response:
		response := botSession.GetResponse()

		jobName, err := bq.botJobMap.Get(ctx, botId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		} else if jobName == nil {
			return nil, status.Errorf(codes.NotFound, "bot not assigned a job")
		}

		job, err := bq.jobStore.Get(ctx, *jobName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		} else if job == nil {
			return nil, status.Errorf(codes.NotFound, "job does not exist")
		}

		if response.Done {
			job.Stage = remoteexecution.ExecuteOperationMetadata_COMPLETED
			job.ExecuteResponse = response.ExecuteResponse

			// TODO(arlyon) transaction and rollback in case of error
			// TODO(arlyon) delete the job after some time
			// we update the job in the store before deletion to allow
			// listeners waiting for its completion to be notified
			err = bq.jobStore.Update(ctx, job)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}

			err = bq.botJobMap.Delete(ctx, botId)
			if err != nil {
				return nil, status.Errorf(codes.Internal, err.Error())
			}
		}
	}

	return &remoteworker.BotSessionResponse{
		Execute: &remoteworker.BotSessionResponse_None{
			None: &empty.Empty{},
		},
	}, nil
}

func (bq *workerBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunction: []remoteexecution.DigestFunction{
				remoteexecution.DigestFunction_MD5,
				remoteexecution.DigestFunction_SHA1,
				remoteexecution.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				// TODO(edsch): Let the frontend flip this to true when enabled?
				UpdateEnabled: false,
			},
			// CachePriorityCapabilities: Priorities not supported.
			// MaxBatchTotalSize: Not used by Bazel yet.
			SymlinkAbsolutePathStrategy: remoteexecution.CacheCapabilities_ALLOWED,
		},
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
			DigestFunction: remoteexecution.DigestFunction_SHA256,
			ExecEnabled:    true,
			ExecutionPriorityCapabilities: &remoteexecution.PriorityCapabilities{
				Priorities: []*remoteexecution.PriorityCapabilities_PriorityRange{
					{MinPriority: math.MinInt32, MaxPriority: math.MaxInt32},
				},
			},
		},
		// TODO(edsch): DeprecatedApiVersion.
		LowApiVersion:  &semver.SemVer{Major: 2},
		HighApiVersion: &semver.SemVer{Major: 2},
	}, nil
}

// TODO(arlyon) transaction and roll back in case of error
func (bq *workerBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	ctx := context.TODO()

	job, existed, err := bq.jobStore.Submit(ctx, in)
	if err != nil {
		return status.Errorf(codes.Internal, "error when submitting execute request: %s", err.Error())
	} else if !existed {
		err = bq.jobQueue.Push(ctx, job)
		if err != nil {
			return status.Errorf(codes.Internal, "error when pushing job to queue: %s", err.Error())
		}
	}

	return job.WaitExecution(ctx, bq.jobNotifier, bq.jobStore, out)
}

func (bq *workerBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	ctx := context.TODO()

	job, err := bq.jobStore.Get(ctx, in.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "error when getting requested job: %s", err.Error())
	} else if job == nil {
		return status.Errorf(codes.NotFound, "build job with name %s not found", in.Name)
	}

	return job.WaitExecution(ctx, bq.jobNotifier, bq.jobStore, out)
}
