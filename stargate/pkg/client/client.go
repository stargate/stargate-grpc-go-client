package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/grpc"
)

type StargateClient struct {
	client pb.StargateClient
}

func NewStargateClientWithConn(conn grpc.ClientConnInterface) (*StargateClient, error) {
	client := pb.NewStargateClient(conn)

	return &StargateClient{
		client: client,
	}, nil
}

func (s *StargateClient) ExecuteQuery(query *pb.Query) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second+10)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second+10)
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
