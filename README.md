# Stargate Golang gRPC Client

This package provides the ability for golang applications to communicate with the [Stargate data gateway](https://stargate.io/)
via gRPC.

- [Quick start guide](#quick-start-guide)
    - [Connecting](#connecting)
    - [Querying](#querying)
    - [Processing the result set](#processing-the-result-set)
- [Running tests](#running-tests)
- [Issue Management](#issue-management)
  
## Quick start guide

To begin, you'll need to add the necessary dependency to your project

```shell
go get -u github.com/stargate/stargate-grpc-go-client
```

If you don't already have access to a Stargate deployment one can be started quickly for testing using the following Docker
command to run Stargate locally in developer mode and expose port 8090 for gRPC connections:

```shell
docker run --name stargate \
  -p 8081:8081 \
  -p 8090:8090 \
  -d \
  -e CLUSTER_NAME=stargate \
  -e CLUSTER_VERSION=3.11 \
  -e DEVELOPER_MODE=true \
  stargateio/stargate-3_11:v1.0.35
```

Ensure the local instance of Stargate is running properly by tailing the logs for the container with `docker logs -f stargate`.
When you see this message, Stargate is ready for traffic:

`Finished starting bundles.`

### Connecting

To connect to your Stargate instance set up the client as follows. This example assumes that you're running Stargate locally
with the default credentials of `cassandra/cassandra`. For more information regarding authentication please see the
[Stargate authentication and authorization docs](https://stargate.io/docs/stargate/1.0/developers-guide/authnz.html).

```go
package main

import (
	"fmt"
	"os"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
	"google.golang.org/grpc"
)

var stargateClient *client.StargateClient

func main() {
	grpcEndpoint := "localhost:8090"
	authEndpoint := "localhost:8081"

	conn, err := grpc.Dial(grpcEndpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithPerRPCCredentials(auth.NewTableBasedTokenProvider(fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra")))
	if err != nil {
		fmt.Printf("error dialing connection %v", err)
		os.Exit(1)
	}

	stargateClient, err = client.NewStargateClientWithConn(conn)
	if err != nil {
		fmt.Printf("error creating client %v", err)
		os.Exit(1)
	}
}
```

### Querying

A simple query can be performed by passing a CQL query to the client

```go
query := &pb.Query{
    Cql: "select cluster_name from system.local",
}

response, err := stargateClient.ExecuteQuery(query)
```

Data definition (DDL) queries are supported in the same manner

```go
// Create a new keyspace
createKeyspaceStatement := &pb.Query{
    Cql: "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
}
_, err = stargateClient.ExecuteQuery(createKeyspaceStatement)
if err != nil {
    return err
}
	
// Create a new table
createTableStatement := `
   CREATE TABLE IF NOT EXISTS ks1.tbl2 (
     key text PRIMARY KEY,
     value text
   );`
createTableQuery := &pb.Query{
	Cql: createTableStatement,
}

_, err = stargateClient.ExecuteQuery(createTableQuery)
if err != nil {
    return err
}
```


Parameterized queries are also supported

```go
any, err := anypb.New(
    &pb.Values{
        Values: []*pb.Value{
            {
                Inner: &pb.Value_String_{
                    String_: "system",
                },
            },
        },
    },
)
if err != nil {
	return err
}

query := &pb.Query{
    Cql: "select * from system_schema.keyspaces where keyspace_name = ?",
    Values: &pb.Payload{
        Type: pb.Payload_CQL,
        Data: any,
    },
    Parameters: &pb.QueryParameters{
        Tracing:      false,
        SkipMetadata: false,
    },
}
response, err := stargateClient.ExecuteQuery(query)
```

If you would like to use a [batch statement](https://cassandra.apache.org/doc/latest/cassandra/cql/dml.html#batch_statement),
the client also provides an `executeBatch()` function for this purpose

```go
batch := &pb.Batch{
    Type:       pb.Batch_LOGGED,
    Queries:    []*pb.BatchQuery{
        {
            Cql: "insert into ks1.tbl2 (key, value) values ('a', 'alpha');",
        },
        {
            Cql: "insert into ks1.tbl2 (key, value) values ('b', 'bravo');",
        },
    },
}

response, err := stargateClient.ExecuteBatch(batch)
```

### Processing the result set

After executing a query a response will be returned containing rows for a SELECT statement, otherwise the returned payload
will be unset. The convenience function `ToResultSet()` is provided to help transform this response into a ResultSet that's easier to work with.

```go
// Insert a record into the table
_, err = stargateClient.ExecuteQuery(&pb.Query{
    Cql: "insert into ks1.tbl2 (key, value) values ('a', 'alpha');",
})
if err != nil {
    return err
}

// Read the data back out
response, err := stargateClient.ExecuteQuery(&pb.Query{
    Cql: "select key, value from ks1.tbl2",
})
if err != nil {
	return err
}

result, err := ToResultSet(response)

// We're calling ToString() here because we know the type being returned. If this was something like a UUID we would use ToUUID().
key, err := ToString(result.Rows[0].Values[0])
if err != nil {
    return err
}

fmt.Printf("key = %s\n", key)
```

Notice that in the above the `ToString` function is used to transform the value into a native string. Additional functions
also exist for other types such as `int`, `map`, and `blob`. The full list can be found in [values.go](stargate/pkg/client/values.go).

## Running tests

The tests for this project can be run from the root using the following command (addition of `-tags integration` will also
run the integration tests).

```shell
go test ./... -v -tags integration
```


## Issue Management

You can reference the [CONTRIBUTING.md](CONTRIBUTING.md) for a full description of how to get involved but the short of it is below.

- If you've found a bug (use the bug label) or want to request a new feature (use the enhancement label), file a GitHub issue
- If you're not sure about it or want to chat, reach out on our [Discord](https://discord.gg/GravUqY) or [mailing list](https://groups.google.com/a/lists.stargate.io/g/stargate-users)
- If you want to write some user docs 🎉 head over to the [stargate/docs](https://github.com/stargate/docs) repo, Pull Requests accepted!
