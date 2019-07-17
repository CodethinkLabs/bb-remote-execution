package store

import (
	"context"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

// workerBuildJob holds the information we need to track for a single
// build action that is enqueued.
type workerBuildJob struct {
	Name             string
	ActionDigest     *remoteexecution.Digest
	DeduplicationKey string
	Timestamp        time.Time

	ExecuteRequest  remoteexecution.ExecuteRequest
	Stage           remoteexecution.ExecuteOperationMetadata_Stage
	ExecuteResponse *remoteexecution.ExecuteResponse
}

func (job *workerBuildJob) WaitExecution(ctx context.Context, notifier JobStateNotifier, store JobStore, out remoteexecution.Execution_ExecuteServer) error {
	for {
		// Send current state to client
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage:        job.Stage,
			ActionDigest: job.ActionDigest,
		})
		if err != nil {
			log.Fatal("Failed to marshal execute operation metadata: ", err)
		}
		operation := &longrunning.Operation{
			Name:     job.Name,
			Metadata: metadata,
		}
		if job.ExecuteResponse != nil {
			operation.Done = true
			response, err := ptypes.MarshalAny(job.ExecuteResponse)
			if err != nil {
				log.Fatal("Failed to marshal execute response: ", err)
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		if err := out.Send(operation); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		// job done
		if job.ExecuteResponse != nil {
			return nil
		}

		// Wait for state transition.
		// TODO(edsch): Should wake up periodically
		if err := notifier.Wait(context.TODO(), job.Name); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		newJob, err := store.Get(ctx, job.Name)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if newJob == nil {
			return status.Errorf(codes.Internal, "job %s does not exist in store", job.Name)
		}

		*job = *newJob
	}
}
