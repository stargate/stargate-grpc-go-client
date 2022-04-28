package client

import (
	"context"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

// StargateQueryExecutor represents an interface that Astra table clients can
// use to execute queries.
type StargateQueryExecutor interface {
	ExecuteQuery(query *pb.Query) (*pb.Response, error)
	ExecuteQueryWithContext(query *pb.Query, ctx context.Context) (
		*pb.Response,
		error,
	)
	ExecuteBatch(batch *pb.Batch) (*pb.Response, error)
	ExecuteBatchWithContext(batch *pb.Batch, ctx context.Context) (
		*pb.Response,
		error,
	)
}
