package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
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
		log.WithError(err).Error("Failed to execute query")
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
		log.WithError(err).Error("Failed to execute query")
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	return resp, nil
}

func ToResultSet(resp *pb.Response) (*pb.ResultSet, error) {
	if resp.GetResultSet() == nil {
		return nil, errors.New("no result set")
	}

	return resp.GetResultSet(), nil
}
