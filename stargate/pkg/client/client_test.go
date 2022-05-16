//go:build integration
// +build integration

package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"gopkg.in/inf.v0"
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
		Image: "stargateio/stargate-3_11:v1.0.40",
		Env: map[string]string{
			"CLUSTER_NAME":    "test",
			"CLUSTER_VERSION": "3.11",
			"DEVELOPER_MODE":  "true",
			"ENABLE_AUTH":     "true",
		},
		ExposedPorts: []string{"8090/tcp", "8081/tcp", "8084/tcp", "9042/tcp"},
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

func TestExecuteQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClient(t)

	query := &pb.Query{
		Cql: "SELECT * FROM system.local",
	}
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.GetResultSet()
	require.NotNil(t, result)

	assert.Equal(t, 18, len(result.Columns))
	assert.Equal(t, &pb.ColumnSpec{
		Type: &pb.TypeSpec{
			Spec: &pb.TypeSpec_Basic_{Basic: 13},
		},
		Name: "key",
	}, result.Columns[0])
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, 18, len(result.Rows[0].Values))

	strVal, err := ToString(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "local", strVal)

	var pagingState []byte
	assert.Equal(t, pagingState, result.PagingState.GetValue())
}

func TestExecuteQuery_AllNumeric(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClient(t)

	query := &pb.Query{
		Cql: "SELECT gc_grace_seconds, default_time_to_live, max_index_interval, memtable_flush_period_in_ms, min_index_interval, read_repair_chance,crc_check_chance,dclocal_read_repair_chance,bloom_filter_fp_chance FROM system_schema.tables",
	}
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.GetResultSet()
	require.NotNil(t, result)

	assert.Equal(t, 9, len(result.Columns))
	assert.Equal(t, &pb.ColumnSpec{
		Type: &pb.TypeSpec{
			Spec: &pb.TypeSpec_Basic_{Basic: 9},
		},
		Name: "gc_grace_seconds",
	}, result.Columns[0])
	assert.GreaterOrEqual(t, len(result.Rows), 37)
	assert.Equal(t, 9, len(result.Rows[0].Values))

	intVal, err := ToInt(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, int64(7776000), intVal)

	var pagingState []byte
	assert.Equal(t, pagingState, result.PagingState.GetValue())
}

func TestExecuteQuery_FullCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClient(t)

	// create keyspace
	query := &pb.Query{
		Cql: "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
	}
	response, err := stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// add table to keyspace
	cql := `
   CREATE TABLE IF NOT EXISTS ks1.tbl1 (
     id uuid PRIMARY KEY,
     asciivalue ascii,
	 textvalue text,
	 varcharvalue varchar,
	 blobvalue blob,
	 booleanvalue boolean,
	 decimalvalue decimal,
	 doublevalue double,
  	 floatvalue float,
	 inetvalue inet,
     bigintvalue bigint,
	 intvalue int,
     smallintvalue smallint,
	 varintvalue varint,
	 tinyintvalue tinyint,
	 timevalue time,
	 timestampvalue timestamp,
     datevalue date,
     timeuuidvalue timeuuid,
     mapvalue map<int,text>,
     listvalue list<text>,
     setvalue set<text>,
     tuplevalue tuple<int, text, float>
   );`
	query = &pb.Query{
		Cql: cql,
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// insert into table
	cql = `
	INSERT INTO ks1.tbl1 (
		id, 
		asciivalue,
		textvalue,
		varcharvalue,
		blobvalue,
		booleanvalue,
		decimalvalue,
		doublevalue,
		floatvalue,
		inetvalue,
		bigintvalue,
		intvalue,
		smallintvalue,
		varintvalue,
		tinyintvalue,
		timevalue,
		timestampvalue,
		datevalue,
		timeuuidvalue,
		mapvalue,
		listvalue,
		setvalue,
		tuplevalue
	) VALUES (
		f066f76d-5e96-4b52-8d8a-0f51387df76b,
		'alpha', 
		'bravo',
		'charlie',
		textAsBlob('foo'),
		true,
		1.1,
        2.2,
		3.3,
		'127.0.0.1',
        1,
		2,
		3,
		4,
		5,
        '10:15:30.123456789',
        '2021-09-07T16:40:31.123Z',
        '2021-09-07',
		30821634-13ad-11eb-adc1-0242ac120002,
		{1: 'a', 2: 'b', 3: 'c'},
		['a', 'b', 'c'],
		{'a', 'b', 'c'},
		(3, 'bar', 2.1)
	);
	`
	query = &pb.Query{
		Cql: cql,
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// read from table
	query = &pb.Query{
		Cql: "SELECT * FROM ks1.tbl1",
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	result := response.GetResultSet()
	require.NotNil(t, result)

	id, err := ToUUID(result.Rows[0].Values[0])
	require.NoError(t, err)
	expectedUUID := uuid.MustParse("f066f76d-5e96-4b52-8d8a-0f51387df76b")
	assert.Equal(t, &expectedUUID, id)

	str, err := ToString(result.Rows[0].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "alpha", str)

	bigint, err := ToBigInt(result.Rows[0].Values[2])
	require.NoError(t, err)
	assert.Equal(t, big.NewInt(int64(1)), bigint)

	blob, err := ToBlob(result.Rows[0].Values[3])
	require.NoError(t, err)
	expectedBytes, _ := hex.DecodeString("666f6f")
	assert.Equal(t, expectedBytes, blob)

	boolean, err := ToBoolean(result.Rows[0].Values[4])
	require.NoError(t, err)
	assert.Equal(t, true, boolean)

	date, err := ToDate(result.Rows[0].Values[5])
	require.NoError(t, err)
	assert.Equal(t, uint32(0x800049bd), date)

	decimal, err := ToDecimal(result.Rows[0].Values[6])
	require.NoError(t, err)
	assert.Equal(t, inf.NewDec(11, 1), decimal)

	double, err := ToDouble(result.Rows[0].Values[7])
	require.NoError(t, err)
	assert.Equal(t, 2.2, double)

	float, err := ToFloat(result.Rows[0].Values[8])
	require.NoError(t, err)
	assert.Equal(t, float32(3.3), float)

	inet, err := ToInet(result.Rows[0].Values[9])
	require.NoError(t, err)
	assert.Equal(t, []byte{0x7f, 0x0, 0x0, 0x1}, inet)

	intVal, err := ToInt(result.Rows[0].Values[10])
	require.NoError(t, err)
	assert.Equal(t, int64(2), intVal)

	listVal, err := ToList(result.Rows[0].Values[11], result.Columns[11].GetType())
	require.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c"}, listVal)

	mapVal, err := ToMap(result.Rows[0].Values[12], result.Columns[12].GetType())
	require.NoError(t, err)
	assert.Equal(t, map[interface{}]interface{}{int64(1): "a", int64(2): "b", int64(3): "c"}, mapVal)

	setVal, err := ToSet(result.Rows[0].Values[13], result.Columns[13].GetType())
	require.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c"}, setVal)

	smallint, err := ToSmallInt(result.Rows[0].Values[14])
	require.NoError(t, err)
	assert.Equal(t, int64(3), smallint)

	str, err = ToString(result.Rows[0].Values[15])
	require.NoError(t, err)
	assert.Equal(t, "bravo", str)

	timestamp, err := ToTimestamp(result.Rows[0].Values[16])
	require.NoError(t, err)
	assert.Equal(t, int64(1631032831123), timestamp)

	timeUUID, err := ToTimeUUID(result.Rows[0].Values[17])
	require.NoError(t, err)
	expectedUUID = uuid.MustParse("30821634-13ad-11eb-adc1-0242ac120002")
	assert.Equal(t, &expectedUUID, timeUUID)

	timeVal, err := ToTime(result.Rows[0].Values[18])
	require.NoError(t, err)
	assert.Equal(t, uint64(0x219676e3e115), timeVal)

	tinyint, err := ToTinyInt(result.Rows[0].Values[19])
	require.NoError(t, err)
	assert.Equal(t, int64(5), tinyint)

	tuple, err := ToTuple(result.Rows[0].Values[20], result.Columns[20].GetType())
	require.NoError(t, err)
	assert.Equal(t, []interface{}{int64(3), "bar", float32(2.1)}, tuple)

	str, err = ToString(result.Rows[0].Values[21])
	require.NoError(t, err)
	assert.Equal(t, "charlie", str)

	varint, err := ToVarInt(result.Rows[0].Values[22])
	require.NoError(t, err)
	assert.Equal(t, uint64(4), varint)

	// update table
	query = &pb.Query{
		Cql: "update ks1.tbl1 set asciivalue = 'echo' WHERE id = f066f76d-5e96-4b52-8d8a-0f51387df76b;",
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// read update from table
	query = &pb.Query{
		Cql: "SELECT * FROM ks1.tbl1 WHERE id = f066f76d-5e96-4b52-8d8a-0f51387df76b;",
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	result = response.GetResultSet()
	require.NotNil(t, result)

	str, err = ToString(result.Rows[0].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "echo", str)
}

func TestExecuteQuery_ParameterizedQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClient(t)

	// read from table
	query := &pb.Query{
		Cql: "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?",
		Values: &pb.Values{
			Values: []*pb.Value{
				{
					Inner: &pb.Value_String_{
						String_: "system",
					},
				},
			},
		},
		Parameters: &pb.QueryParameters{
			Tracing:      false,
			SkipMetadata: false,
		},
	}
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.GetResultSet()
	require.NotNil(t, result)

	assert.Equal(t, 1, len(result.Rows))

	strVal, err := ToString(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "system", strVal)
}

func TestExecuteBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClient(t)

	// create keyspace
	query := &pb.Query{
		Cql: "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
	}
	response, err := stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// add table to keyspace
	cql := `
   CREATE TABLE IF NOT EXISTS ks1.tbl2 (
     key text PRIMARY KEY,
     value text
   );`
	query = &pb.Query{
		Cql: cql,
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	batch := &pb.Batch{
		Type: pb.Batch_LOGGED,
		Queries: []*pb.BatchQuery{
			{
				Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('a', 'alpha');",
			},
			{
				Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('b', 'bravo');",
			},
		},
	}

	response, err = stargateClient.ExecuteBatch(batch)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// read from table
	query = &pb.Query{
		Cql: "SELECT * FROM ks1.tbl2",
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	result := response.GetResultSet()
	require.NotNil(t, result)

	key, err := ToString(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "a", key)

	value, err := ToString(result.Rows[0].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "alpha", value)

	key, err = ToString(result.Rows[1].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "b", key)

	value, err = ToString(result.Rows[1].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "bravo", value)

	// update table
	batch = &pb.Batch{
		Type: pb.Batch_LOGGED,
		Queries: []*pb.BatchQuery{
			{
				Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('c', 'charlie');",
			},
			{
				Cql: "UPDATE ks1.tbl2 SET value = 'bagel' WHERE key = 'b';",
			},
		},
	}
	response, err = stargateClient.ExecuteBatch(batch)
	require.NoError(t, err)

	assert.Nil(t, response.GetResultSet())

	// read update from table
	query = &pb.Query{
		Cql: "SELECT * FROM ks1.tbl2 WHERE key in ('b', 'c');",
	}
	response, err = stargateClient.ExecuteQuery(query)
	require.NoError(t, err)

	result = response.GetResultSet()
	require.NotNil(t, result)

	key, err = ToString(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "b", key)

	value, err = ToString(result.Rows[0].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "bagel", value)

	key, err = ToString(result.Rows[1].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "c", key)

	value, err = ToString(result.Rows[1].Values[1])
	require.NoError(t, err)
	assert.Equal(t, "charlie", value)
}

func TestExecuteQuery_UsingStaticToken(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	stargateClient := createClientWithStaticToken(t)

	query := &pb.Query{
		Cql: "select * from system.local",
	}
	response, err := stargateClient.ExecuteQuery(query)
	if err != nil {
		assert.FailNow(t, "Should not have returned error", err)
	}

	result := response.GetResultSet()
	require.NotNil(t, result)

	assert.Equal(t, 18, len(result.Columns))
	assert.Equal(t, &pb.ColumnSpec{
		Type: &pb.TypeSpec{
			Spec: &pb.TypeSpec_Basic_{Basic: 13},
		},
		Name: "key",
	}, result.Columns[0])
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, 18, len(result.Rows[0].Values))

	strVal, err := ToString(result.Rows[0].Values[0])
	require.NoError(t, err)
	assert.Equal(t, "local", strVal)

	var pagingState []byte
	assert.Equal(t, pagingState, result.PagingState.GetValue())
}

func TestTimeoutOption(t *testing.T) {
	conn, err := grpc.Dial(grpcEndpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithPerRPCCredentials(
			auth.NewTableBasedTokenProviderUnsafe(
				fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra",
			),
		),
	)
	require.NoError(t, err)

	s, err := NewStargateClientWithConn(conn)
	require.NoError(t, err)
	assert.Equal(t, defaultTimeout, s.timeout)

	s, err = NewStargateClientWithConn(conn, WithTimeout(time.Second*2))
	require.NoError(t, err)
	assert.Equal(t, time.Second*2, s.timeout)
}

func createClient(t *testing.T) *StargateClient {
	conn, err := grpc.Dial(grpcEndpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithPerRPCCredentials(
			auth.NewTableBasedTokenProviderUnsafe(
				fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra",
			),
		),
	)
	require.NoError(t, err)

	stargateClient, err := NewStargateClientWithConn(conn)
	require.NoError(t, err)
	return stargateClient
}

func createClientWithStaticToken(t *testing.T) *StargateClient {
	token, err := getAuthToken()
	require.NoError(t, err)

	conn, err := grpc.Dial(grpcEndpoint, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithPerRPCCredentials(auth.NewStaticTokenProviderUnsafe(token)))
	require.NoError(t, err)

	stargateClient, err := NewStargateClientWithConn(conn)
	require.NoError(t, err)
	return stargateClient
}

func getAuthToken() (string, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, fmt.Sprintf("http://%s/v1/auth", authEndpoint), strings.NewReader("{\"username\": \"cassandra\",\"password\": \"cassandra\"}"))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error calling auth service: %v", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Warnf("unable to close response body: %v", err)
		}
	}()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %v", err)
	}

	var result map[string]string
	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling response body: %v", err)
	}

	return result["authToken"], nil
}
