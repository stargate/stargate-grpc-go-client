package client

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

type StargateClient struct {
	client pb.StargateClient
	conn   *grpc.ClientConn
}

type Parameters struct {
	Keyspace          string
	Consistency       Consistency
	PageSize          int32
	PagingState       []byte
	Tracing           bool
	SkipMetadata      bool
	Timestamp         int64
	SerialConsistency SerialConsistency
	NowInSeconds      int32
}

func NewStargateClient(target string) (*StargateClient, error) {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		// TODO: [doug] return err here
		log.Println(err)
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	client := pb.NewStargateClient(conn)

	return &StargateClient{
		client: client,
		conn:   conn,
	}, nil
}

type Query struct {
	Cql        string
	Values     interface{}
	Parameters Parameters
}

func NewQuery() *Query {
	return &Query{
		Parameters: Parameters{
			Consistency:       UNSET,
			SerialConsistency: UNSET_SERIAL,
		},
	}
}

func (s *StargateClient) ExecuteQuery(query *Query) (*ResultSet, error) {
	// TODO: [doug] configurable timeout?
	ctx, cancel := context.WithTimeout(context.Background(), time.Second+10)
	defer cancel()

	md := metadata.New(map[string]string{"x-cassandra-token": auth.GetToken()})
	ctx = metadata.NewOutgoingContext(ctx, md)

	in := &pb.Query{
		Cql:        query.Cql,
		Values:     buildPayload(query.Values),
		Parameters: buildQueryParameters(query.Parameters),
	}

	resp, err := s.client.ExecuteQuery(ctx, in)
	if err != nil {
		log.WithError(err).Error("Failed to execute query")
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	data := resp.ResultSet.Data

	var resultSet pb.ResultSet
	if err := anypb.UnmarshalTo(data, &resultSet, proto.UnmarshalOptions{}); err != nil {
		log.WithError(err).Error("Could not unmarshal result")
		return nil, fmt.Errorf("could not unmarshal result: %v", err)
	}

	var result ResultSet
	result.Columns = []*ColumnSpec{}
	result.Rows = []*Row{}

	for i, row := range resultSet.Rows {
		result.Rows = append(result.Rows, &Row{Values: []*Value{}})
		for j, v := range row.Values {
			columnSpec := resultSet.Columns[j]
			unmarshalRowValue(columnSpec, v, result, i)
		}
	}
	return &result, nil
}

func unmarshalRowValue(columnSpec *pb.ColumnSpec, value *pb.Value, result ResultSet, rowIndex int) {
	switch columnSpec.Type.GetSpec().(type) {
	case *pb.TypeSpec_Basic_:
		handleBasicType(columnSpec, value, result, rowIndex)
	case *pb.TypeSpec_Map_:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetCollection()).Debug("map")
	case *pb.TypeSpec_List_:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetCollection()).Debug("list")
	case *pb.TypeSpec_Set_:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetCollection()).Debug("set")
	case *pb.TypeSpec_Udt_:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetUdt()).Debug("udt")
	case *pb.TypeSpec_Tuple_:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetCollection()).Debug("tuple")
	}
}

func handleBasicType(columnSpec *pb.ColumnSpec, value *pb.Value, result ResultSet, rowIndex int) {
	var (
		column *ColumnSpec
		val *Value
	)

	switch columnSpec.Type.GetBasic() {
	case pb.TypeSpec_ASCII:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetString_()).Debug("ascii")

		column = &ColumnSpec{
			TypeSpec: TypeSpecBasic{ASCII},
			Name: columnSpec.Name,
		}
		val = &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	case pb.TypeSpec_TEXT:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetString_()).Debug("text")

		column = &ColumnSpec{
			TypeSpec: TypeSpecBasic{TEXT},
			Name: columnSpec.Name,
		}
		val = &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	case pb.TypeSpec_VARCHAR:
		log.WithField("name", columnSpec.Name).WithField("value", value.GetString_()).Debug("varchar")

		column = &ColumnSpec{
			TypeSpec: TypeSpecBasic{VARCHAR},
			Name: columnSpec.Name,
		}
		val = &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	}

	result.Columns = append(result.Columns, column)
	result.Rows[rowIndex].Values = append(result.Rows[rowIndex].Values, val)
}

func buildPayload(values interface{}) *pb.Payload {
	return &pb.Payload{
		Type: 0,
		Data: nil,
	}
}

func buildQueryParameters(parameters Parameters) *pb.QueryParameters {
	params := &pb.QueryParameters{
		Tracing:      parameters.Tracing,
		SkipMetadata: parameters.SkipMetadata,
	}

	if parameters.Keyspace != "" {
		params.Keyspace = wrapperspb.String(parameters.Keyspace)
	}

	if parameters.Consistency == UNSET {
		params.Consistency = convertConsistency(ONE)
	} else {
		params.Consistency = convertConsistency(parameters.Consistency)
	}

	if parameters.PageSize > 0 {
		params.PageSize = wrapperspb.Int32(parameters.PageSize)
	}

	if parameters.PagingState != nil {
		params.PagingState = wrapperspb.Bytes(parameters.PagingState)
	}

	if parameters.Timestamp > 0 {
		params.Timestamp = wrapperspb.Int64(parameters.Timestamp)
	}

	if parameters.SerialConsistency != UNSET_SERIAL {
		params.SerialConsistency = convertSerialConsistency(parameters.SerialConsistency)
	}

	if parameters.NowInSeconds > 0 {
		params.NowInSeconds = wrapperspb.Int32(parameters.NowInSeconds)
	}

	return params
}

func (s StargateClient) Close() {
	err := s.conn.Close()
	if err != nil {
		log.Printf("unable to close connection: %v", err)
	}
}