package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/grpc"
)

const defaultTimeout = time.Second * 10

type StargateClient struct {
	client  pb.StargateClient
	timeout time.Duration
}

// StargateClientOption is an option for a StargateClient.
type StargateClientOption func(*StargateClient)

// NewStargateClientWithConn creates a new StargateClient with the specified
// gRPC connection and options.
func NewStargateClientWithConn(
	conn grpc.ClientConnInterface,
	opts ...StargateClientOption,
) (*StargateClient, error) {
	c := pb.NewStargateClient(conn)
	sc := &StargateClient{
		client:  c,
		timeout: defaultTimeout,
	}

	for _, opt := range opts {
		opt(sc)
	}

	return sc, nil
}

func (s *StargateClient) ExecuteQuery(query *pb.Query) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	return s.ExecuteQueryWithContext(query, ctx)
}

func (s *StargateClient) ExecuteQueryWithContext(query *pb.Query, ctx context.Context) (*pb.Response, error) {
	resp, err := s.client.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	return resp, nil
}

func (s *StargateClient) ExecuteBatch(batch *pb.Batch) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	return s.ExecuteBatchWithContext(batch, ctx)
}

func (s *StargateClient) ExecuteBatchWithContext(batch *pb.Batch, ctx context.Context) (*pb.Response, error) {
	resp, err := s.client.ExecuteBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	return resp, nil
}

// WithTimeout returns a StargateClientOption which sets the context timeout for
// client requests.
func WithTimeout(timeout time.Duration) StargateClientOption {
	return func(c *StargateClient) {
		c.timeout = timeout
	}
}
