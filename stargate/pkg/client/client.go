package client

import (
	"context"
	"errors"
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
	client       pb.StargateClient
	conn         *grpc.ClientConn
	authProvider auth.AuthProviderIFace
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

func NewStargateClient(target string, authProvider auth.AuthProviderIFace) (*StargateClient, error) {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.WithError(err).Error("Failed to create client")
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	client := pb.NewStargateClient(conn)

	return &StargateClient{
		client:       client,
		conn:         conn,
		authProvider: authProvider,
	}, nil
}

type Query struct {
	Cql        string
	Values     interface{}
	Parameters Parameters
}

type Batch struct{}

func NewQuery() *Query {
	return &Query{
		Parameters: Parameters{
			Consistency:       UNSET,
			SerialConsistency: UNSET_SERIAL,
		},
	}
}

func (s *StargateClient) ExecuteQuery(query *Query) (*Response, error) {
	// TODO: [doug] configurable timeout?
	ctx, cancel := context.WithTimeout(context.Background(), time.Second+10)
	defer cancel()

	token, err := s.authProvider.GetToken()
	if err != nil {
		log.WithError(err).Error("Failed to get auth token")
		return nil, fmt.Errorf("failed to get auth token: %v", err)
	}
	md := metadata.New(map[string]string{"x-cassandra-token": token})
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

	response := &Response{
		TracingId: resp.TracingId,
		Warnings:  resp.Warnings,
	}

	if resp.ResultSet == nil {
		// Valid for not all requests to have a ResultSet (e.g. schema changes). Since we've made it this far the request
		// was successful so just return warnings and tracing info
		return response, nil
	}

	data := resp.ResultSet.Data

	var resultSet pb.ResultSet
	if err := anypb.UnmarshalTo(data, &resultSet, proto.UnmarshalOptions{}); err != nil {
		log.WithError(err).Error("Could not unmarshal result")
		return nil, fmt.Errorf("could not unmarshal result: %v", err)
	}

	var result ResultSet
	result.Rows = []*Row{}
	for i, row := range resultSet.Rows {
		result.Rows = append(result.Rows, &Row{Values: []*Value{}})
		for j, v := range row.Values {
			result.Rows[i].Values = append(result.Rows[i].Values, translateType(resultSet.Columns[j].Type, v))
		}
	}

	result.Columns = []*ColumnSpec{}
	for _, col := range resultSet.Columns {
		result.Columns = append(result.Columns, columnSpecFromProto(col))
	}

	response.ResultSet = &result
	return response, nil
}

func (s *StargateClient) ExecuteBatch(batch *Batch) (*Response, error) {
	return nil, errors.New("not yet implemented")
}

func columnSpecFromProto(col *pb.ColumnSpec) *ColumnSpec {
	return &ColumnSpec{
		TypeSpec: mapTypeSpec(col.Type),
		Name:     col.Name,
	}
}

func buildPayload(values interface{}) *pb.Payload {
	// TODO: [doug] implement this
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
