package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type StargateClient struct {
	client pb.StargateClient
	conn   grpc.ClientConnInterface
}

func NewStargateClientWithConn(conn grpc.ClientConnInterface) (*StargateClient, error) {
	client := pb.NewStargateClient(conn)

	return &StargateClient{
		client: client,
		conn:   conn,
	}, nil
}

type Batch struct{}

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

func (s *StargateClient) ExecuteBatch(batch *Batch) (*pb.Response, error) {
	return nil, errors.New("not yet implemented")
}

func (s *StargateClient) ExecuteBatchWithContext(batch *Batch, ctx context.Context) (*pb.Response, error) {
	return nil, errors.New("not yet implemented")
}

func ToResultSet(resp *pb.Response) (*pb.ResultSet, error) {
	if resp.GetResultSet() == nil {
		return nil, errors.New("no result set")
	}

	data := resp.GetResultSet().Data
	var resultSet pb.ResultSet
	if err := anypb.UnmarshalTo(data, &resultSet, proto.UnmarshalOptions{}); err != nil {
		log.WithError(err).Error("Could not unmarshal result")
		return nil, fmt.Errorf("could not unmarshal result: %v", err)
	}
	return &resultSet, nil
}
