package client

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

func TestNewQuery(t *testing.T) {
	stargateClient, err := NewStargateClient("localhost:8090", auth.NewTableBasedTokenProvider())
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
	stargateClient, err := NewStargateClient("localhost:8090", auth.NewTableBasedTokenProvider())
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

func TestNewQuery_DoStuff(t *testing.T) {
	stargateClient, err := NewStargateClient("localhost:8090", auth.NewTableBasedTokenProvider())
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
