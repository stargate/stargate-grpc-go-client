// +build integration

package client

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
)

var (
	grpcEndpoint string
	authEndpoint string
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)

	log.Info("Setting up test containers")

	ctx := context.Background()
	waitStrategy := wait.ForHTTP("/checker/readiness").WithPort("8084/tcp").WithStartupTimeout(90 * time.Second)

	req := testcontainers.ContainerRequest{
		Image: "stargateio/stargate-3_11:v1.0.28",
		Env: map[string]string{
			"CLUSTER_NAME":    "test",
			"CLUSTER_VERSION": "3.11",
			"DEVELOPER_MODE":  "true",
			"ENABLE_AUTH":  "true",
		},
		ExposedPorts: []string{"8090/tcp", "8081/tcp", "8084/tcp"},
		WaitingFor:   waitStrategy,
	}
	stargateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		log.Fatalf("Failed to start Stargate container: %v", err)
	}

	grpcPort, err := nat.NewPort("tcp", "8090")
	if err != nil {
		log.Fatalf("Failed to get port: %v", err)
	}
	authPort, err := nat.NewPort("tcp", "8081")
	if err != nil {
		log.Fatalf("Failed to get port: %v", err)
	}

	grpcEndpoint, err = stargateContainer.PortEndpoint(ctx, grpcPort, "")
	if err != nil {
		log.Fatalf("Failed to get endpoint: %v", err)
	}

	authEndpoint, err = stargateContainer.PortEndpoint(ctx, authPort, "")
	if err != nil {
		log.Fatalf("Failed to get endpoint: %v", err)
	}
}

func TestNewQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient, err := NewStargateClient(grpcEndpoint, auth.NewTableBasedTokenProvider(fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra"))
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	query := NewQuery()
	query.Cql = "select * from system.local"
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.ResultSet

	assert.Equal(t, 18, len(result.Columns))
	assert.Equal(t, &ColumnSpec{
		TypeSpec: TypeSpecBasic{VARCHAR},
		Name:     "key",
	}, result.Columns[0])
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, 18, len(result.Rows[0].Values))
	assert.Equal(t, &Value{
		Inner: ValueString{
			String: "local",
		},
	}, result.Rows[0].Values[0])
	assert.Equal(t, []byte(nil), result.PagingState)
	assert.Equal(t, int32(0), result.PageSize)
}

func TestNewQuery_AllNumeric(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient, err := NewStargateClient(grpcEndpoint, auth.NewTableBasedTokenProvider(fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra"))
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	query := NewQuery()
	query.Cql = "select gc_grace_seconds, default_time_to_live, max_index_interval, memtable_flush_period_in_ms, min_index_interval, read_repair_chance,crc_check_chance,dclocal_read_repair_chance,bloom_filter_fp_chance from system_schema.tables"
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.ResultSet

	assert.Equal(t, 9, len(result.Columns))
	assert.Equal(t, &ColumnSpec{
		TypeSpec: TypeSpecBasic{INT},
		Name:     "gc_grace_seconds",
	}, result.Columns[0])
	assert.GreaterOrEqual(t, len(result.Rows), 37)
	assert.Equal(t, 9, len(result.Rows[0].Values))
	assert.Equal(t, &Value{
		Inner: ValueInt{Int: int64(7776000)},
	}, result.Rows[0].Values[0])
	assert.Equal(t, []byte(nil), result.PagingState)
	assert.Equal(t, int32(0), result.PageSize)
}

func TestNewQuery_FullCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient, err := NewStargateClient(grpcEndpoint, auth.NewTableBasedTokenProvider(fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra"))
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	var unsetResultSet *ResultSet

	// create keyspace
	query := NewQuery()
	query.Cql = "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};"
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}
	assert.Equal(t, unsetResultSet, response.ResultSet)

	// add table to keyspace
	query = NewQuery()
	query.Cql = `
    CREATE TABLE IF NOT EXISTS ks1.tbl1 (
      key text,
      value int,
      PRIMARY KEY (key)
    );`
	response, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	assert.Equal(t, unsetResultSet, response.ResultSet)

	// insert into table
	query = NewQuery()
	query.Cql = "insert into ks1.tbl1 (key, value) values ('alpha', 1);"
	response, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}
	assert.Equal(t, unsetResultSet, response.ResultSet)

	// read from table
	query = NewQuery()
	query.Cql = "select * from ks1.tbl1;"
	response, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	assert.Equal(t, &Value{Inner: ValueString{String: "alpha"}}, response.ResultSet.Rows[0].Values[0])
	assert.Equal(t, &Value{Inner: ValueInt{Int: 1}}, response.ResultSet.Rows[0].Values[1])

	// update table
	query = NewQuery()
	query.Cql = "update ks1.tbl1 set value = 2 where key = 'alpha';"
	response, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}
	assert.Equal(t, unsetResultSet, response.ResultSet)

	// read update from table
	query = NewQuery()
	query.Cql = "select * from ks1.tbl1;"
	response, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	assert.Equal(t, &Value{Inner: ValueString{String: "alpha"}}, response.ResultSet.Rows[0].Values[0])
	assert.Equal(t, &Value{Inner: ValueInt{Int: 2}}, response.ResultSet.Rows[0].Values[1])
}

func TestNewQuery_ParameterizedQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient, err := NewStargateClient(grpcEndpoint, auth.NewTableBasedTokenProvider(fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra"))
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	// read from table
	query := NewQuery()
	query.Cql = "select * from system_schema.keyspaces where keyspace_name = ?"
	query.Values = Payload{
		Type: Payload_CQL,
		Data: []Value{{Inner: ValueString{String: "system"}}},
	}
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	assert.Equal(t, 1, len(response.ResultSet.Rows))
	assert.Equal(t, &Value{Inner: ValueString{String: "system"}}, response.ResultSet.Rows[0].Values[0])
}
