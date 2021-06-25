package client

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

func TestNewQuery(t *testing.T) {
	stargateClient, err := NewStargateClient("localhost:8090")
	if err != nil {
		assert.Fail(t, "Should not have returned error", err)
	}

	query := NewQuery()
	query.Cql = "select * from system.local"
	result, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.Fail(t, "Should not have returned error", err)
	}

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

func TestNewQueryAllNumeric(t *testing.T) {
	stargateClient, err := NewStargateClient("localhost:8090")
	if err != nil {
		assert.Fail(t, "Should not have returned error", err)
	}

	query := NewQuery()
	query.Cql = "select gc_grace_seconds, default_time_to_live, max_index_interval, memtable_flush_period_in_ms, min_index_interval, read_repair_chance,crc_check_chance,dclocal_read_repair_chance,bloom_filter_fp_chance from system_schema.tables"
	result, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.Fail(t, "Should not have returned error", err)
	}

	assert.Equal(t, 9, len(result.Columns))
	assert.Equal(t, &ColumnSpec{
		TypeSpec: TypeSpecBasic{INT},
		Name:     "gc_grace_seconds",
	}, result.Columns[0])
	assert.Equal(t, 37, len(result.Rows))
	assert.Equal(t, 9, len(result.Rows[0].Values))
	assert.Equal(t, &Value{
		Inner: ValueInt{Int: int64(7776000)},
	}, result.Rows[0].Values[0])
	assert.Equal(t, []byte(nil), result.PagingState)
	assert.Equal(t, int32(0), result.PageSize)
}