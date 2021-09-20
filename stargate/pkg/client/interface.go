package client

import pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

type StargateClientIFace interface {
	ExecuteQuery(query *pb.Query) (*pb.Response, error)
	ExecuteBatch(batch *pb.Batch) (*pb.Response, error)
}
