package main

import (
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
	//log.SetFormatter(&log.JSONFormatter{})
}

func main() {
	stargateClient, err := client.NewStargateClient("localhost:8090")
	if err != nil {
		log.Fatal(err)
	}

	query := client.NewQuery()
	query.Cql = "select * from system.local"
	_, err = stargateClient.ExecuteQuery(query)
	if err != nil {
		log.Fatal(err)
	}
}
