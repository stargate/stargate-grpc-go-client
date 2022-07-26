# Stargate Golang gRPC Client

This package provides the ability for golang applications to communicate with the [Stargate data gateway](https://stargate.io/)
via gRPC.

- [Quick start guide](#quick-start-guide)
    - [Connecting](#connecting)
    - [Querying](#querying)
    - [Processing the result set](#processing-the-result-set)
- [Issue Management](#issue-management)
  
## Quick start guide

To begin, you'll need to add the necessary dependency to your project:

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
  stargateio/stargate-3_11:v1.0.61
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
    "log"
    "context"

    "github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
    "github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

var stargateClient *client.StargateClient

func main() {
    grpcEndpoint := "localhost:8090"
    authEndpoint := "localhost:8081"

    // Create a context to add a timeout to the gRPC dial
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    conn, err := grpc.DialContext(ctx, grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
      grpc.WithPerRPCCredentials(
        auth.NewTableBasedTokenProviderUnsafe(
          fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra",
        ),
      ),
    )
    if err != nil {
        log.Fatalf("error dialing connection %v", err)
    }

    stargateClient, err = client.NewStargateClientWithConn(conn)
    if err != nil {
        log.Fatalf("error creating client %v", err)
    }
}
```

In a secure environment you'll dial the connection like this:

```go
config := &tls.Config{}
conn, err := grpc.DialContext(ctx, grpcEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(config)), 
    grpc.WithBlock(),
    grpc.WithPerRPCCredentials(
        auth.NewTableBasedTokenProvider(
          fmt.Sprintf("https://%s/v1/auth", authEndpoint), "cassandra", "cassandra",
        ),
    ),
)
```



### Querying

A simple query can be performed by passing a CQL query to the client:

```go
query := &pb.Query{
    Cql: "SELECT cluster_name FROM system.local",
}

response, err := stargateClient.ExecuteQuery(query)
```

Data definition (DDL) queries are supported in the same manner:

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


Parameterized queries are also supported:

```go
query := &pb.Query{
    Cql: "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?",
    Values:  &pb.Values{
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
```

If you would like to use a [batch statement](https://cassandra.apache.org/doc/latest/cassandra/cql/dml.html#batch_statement),
the client also provides an `ExecuteBatch()` function for this purpose:

```go
batch := &pb.Batch{
    Type:       pb.Batch_LOGGED,
    Queries:    []*pb.BatchQuery{
        {
            Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('a', 'alpha');",
        },
        {
            Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('b', 'bravo');",
        },
    },
}

response, err := stargateClient.ExecuteBatch(batch)
```

#### Query Timeouts

By default, all queries will time out after 10 seconds. You can customize this behavior at a per-query level using the `ExecuteQueryWithContext` and `ExecuteBatchWithContext` functions:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

response, err := stargateClient.ExecuteQueryWithContext(query, ctx)
```

You can also set a per-call timeout once when constructing the client:

```go
stargateClient, err := client.NewStargateClientWithConn(conn, client.WithTimeout(3*time.Second))
```

### Processing the result set

After executing a query a response will be returned containing rows for a SELECT statement, otherwise the returned payload
will be unset:

```go
// Insert a record into the table
_, err = stargateClient.ExecuteQuery(&pb.Query{
    Cql: "INSERT INTO ks1.tbl2 (key, value) VALUES ('a', 'alpha');",
})
if err != nil {
    return err
}

// Read the data back out
response, err := stargateClient.ExecuteQuery(&pb.Query{
    Cql: "SELECT key, value FROM ks1.tbl2",
})
if err != nil {
    return err
}

result := response.GetResultSet()

// We're calling ToString() here because we know the type being returned. If this was
// something like a UUID we would use ToUUID().
key, err := ToString(result.Rows[0].Values[0])
if err != nil {
    return err
}

fmt.Printf("key = %s\n", key)
```

Notice that in the above the `ToString` function is used to transform the value into a native string. Additional functions
also exist for other types such as `int`, `map`, and `blob`. The full list can be found in [values.go](stargate/pkg/client/values.go).

## Issue Management

You can reference the [CONTRIBUTING.md](CONTRIBUTING.md) for a full description of how to get involved but the short of it is below.

- If you've found a bug (use the bug label) or want to request a new feature (use the enhancement label), file a GitHub issue
- If you're not sure about it or want to chat, reach out on our [Discord](https://discord.gg/GravUqY) 
- If you want to write some user docs 🎉 head over to the [stargate/docs](https://github.com/stargate/docs) repo, Pull Requests accepted!
