package worker

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	remoteworker "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
)

// Client contains everything needed to contact the scheduler and execute tasks
type Client struct {
	botSession     *remoteworker.BotSessionSend
	executeRequest *remoteexecution.ExecuteRequest
	botClient      remoteworker.BotClient
	buildExecutor  builder.BuildExecutor
	browserURL     *url.URL
}

// NewClient creates a new client for a worker
func NewClient(botClient remoteworker.BotClient, buildExecutor builder.BuildExecutor, browserURL *url.URL, instanceName string, platform *remoteexecution.Platform) *Client {

	hostname, _ := os.Hostname()
	creationTime := time.Now()
	creationProto, _ := ptypes.TimestampProto(creationTime)

	botId := &remoteworker.BotId{
		Hostname:          hostname,
		CreationTimestamp: creationProto,
		Uuid:              uuid.Must(uuid.NewRandom()).String(),
	}

	botSession := &remoteworker.BotSessionSend{
		BotId:        botId,
		InstanceName: instanceName,
		Platform:     platform,
	}

	return &Client{
		botSession:    botSession,
		botClient:     botClient,
		buildExecutor: buildExecutor,
		browserURL:    browserURL,
	}
}

func (wc *Client) heartbeat(ticker *time.Ticker) {
	for range ticker.C {
		wc.Update()
	}
}

// BotSession with the server.
func (wc *Client) BotSession() error {
	wc.botSession.Execute = &remoteworker.BotSessionSend_None{
		None: &empty.Empty{},
	}

	botSessionResponse, err := wc.Update()
	if err != nil {
		return err
	}

	switch botSessionResponse.Execute.(type) {
	case *remoteworker.BotSessionResponse_None:
		// No work to execute
		return nil

	case *remoteworker.BotSessionResponse_Request:
		wc.executeRequest = botSessionResponse.GetRequest()
		wc.botSession.Execute = &remoteworker.BotSessionSend_Response{
			Response: &remoteworker.BotResponse{
				Done: false,
			},
		}
	}

	// Print URL of the action into the log before execution.
	actionURL, err := wc.browserURL.Parse(
		fmt.Sprintf(
			"/action/%s/%s/%d/",
			wc.botSession.InstanceName,
			wc.executeRequest.ActionDigest.Hash,
			wc.executeRequest.ActionDigest.SizeBytes))
	if err != nil {
		return err
	}

	log.Print("Action: ", actionURL.String())

	// TODO
	// Take this from the server
	pollingInterval := uint64(60)

	ticker := time.NewTicker(time.Duration(pollingInterval) * time.Second)
	go wc.heartbeat(ticker)

	executeResponse, _ := wc.buildExecutor.Execute(context.Background(), wc.executeRequest)
	ticker.Stop()

	wc.botSession.Execute = &remoteworker.BotSessionSend_Response{
		Response: &remoteworker.BotResponse{
			ExecuteResponse: executeResponse,
			Done:            true,
		},
	}

	if _, err := wc.Update(); err != nil {
		return err
	}

	return err
}

// Update the bot session.
func (wc *Client) Update() (*remoteworker.BotSessionResponse, error) {
	return wc.botClient.Update(context.Background(), wc.botSession)
}
