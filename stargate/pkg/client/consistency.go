package client

import pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

type Consistency int32
type SerialConsistency int32

const (
	UNSET        Consistency = -1
	ANY          Consistency = 0
	ONE          Consistency = 1
	TWO          Consistency = 2
	THREE        Consistency = 3
	QUORUM       Consistency = 4
	ALL          Consistency = 5
	LOCAL_QUORUM Consistency = 6
	EACH_QUORUM  Consistency = 7
	LOCAL_ONE    Consistency = 10
)

const (
	UNSET_SERIAL SerialConsistency = -1
	SERIAL       SerialConsistency = 8
	LOCAL_SERIAL SerialConsistency = 9
)

func convertConsistency(consistency Consistency) *pb.ConsistencyValue {
	return &pb.ConsistencyValue{
		Value: getConsistency(consistency),
	}
}

func getConsistency(value Consistency) pb.Consistency {
	switch value {
	case ANY:
		return pb.Consistency_ANY
	case ONE:
		return pb.Consistency_ONE
	case TWO:
		return pb.Consistency_TWO
	case THREE:
		return pb.Consistency_THREE
	case QUORUM:
		return pb.Consistency_QUORUM
	case ALL:
		return pb.Consistency_ALL
	case LOCAL_QUORUM:
		return pb.Consistency_LOCAL_QUORUM
	case EACH_QUORUM:
		return pb.Consistency_EACH_QUORUM
	case LOCAL_ONE:
		return pb.Consistency_LOCAL_ONE
	}
	return pb.Consistency_ONE
}

func convertSerialConsistency(serialConsistency SerialConsistency) *pb.ConsistencyValue {
	return &pb.ConsistencyValue{
		Value: getSerialConsistency(serialConsistency),
	}
}

func getSerialConsistency(value SerialConsistency) pb.Consistency {
	switch value {
	case SERIAL:
		return pb.Consistency_SERIAL
	case LOCAL_SERIAL:
		return pb.Consistency_LOCAL_SERIAL
	}
	return pb.Consistency_SERIAL
}
