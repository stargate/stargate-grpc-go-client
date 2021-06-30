package client

import pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

type Query struct {
	Cql        string
	Values     Payload
	Parameters Parameters
}

type Payload struct {
	Type Payload_Type
	Data []Value
}

type Payload_Type int32

const (
	Payload_CQL Payload_Type = 0
)

func (p Payload_Type) toProtoType() pb.Payload_Type {
	switch p {
	case Payload_CQL:
		return pb.Payload_CQL
	default:
		return pb.Payload_CQL
	}
}
