package store

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// JobStore is an interface for a map that guarantees uniqueness
// of both the input strings and some deduplication function.
type JobStore interface {

	// Submit adds an ExecuteRequest to the JobStore
	Submit(ctx context.Context, req *remoteexecution.ExecuteRequest) (job *workerBuildJob, existed bool, err error)

	// Get looks up a job by its name
	Get(ctx context.Context, jobName string) (job *workerBuildJob, err error)

	// Delete removes a job by its name
	Delete(ctx context.Context, jobName string) error

	// Update amends the data for a given job, or
	// errors if the job does not already exist.
	Update(ctx context.Context, job *workerBuildJob) error
}

type JobStateNotifier interface {
	Wait(ctx context.Context, jobName string) error
}

// JobQueue is a heap of workerBuildJob entries, sorted by
// priority in which they should be executed. If all nodes
// have a priority of 0, this is essentially a FILO
type JobQueue interface {

	// Push puts a new job on the queue.
	Push(ctx context.Context, job *workerBuildJob) error

	// Pop gets the next job from the queue. Note that this
	// operation will wait for an element if needed.
	Pop(ctx context.Context) (jobName *string, err error)
}

type BotJobMap interface {
	Put(ctx context.Context, botId string, jobId string) error
	Delete(ctx context.Context, botId string) error
	Get(ctx context.Context, botId string) (jobId *string, err error)
}
