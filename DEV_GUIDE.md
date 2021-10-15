# Developer Guide

## Getting started

Clone the repo, then install dependencies:

```shell
go mod download
```

## Running tests

The tests for this project can be run from the root using the following command (addition of `-tags integration` will also
run the integration tests).

```shell
go test ./... -v -tags integration
```

## Generating gRPC code stubs

To update the protobuf files being used add the new files to the top level proto directory and then run `make proto` from
the root of the project. After running, you will find the new generated `*.pb.go` files in `stargate/pkg/proto`

## Coding style

This project uses [golangci-lint](https://github.com/golangci/golangci-lint) to lint code. These standards are enforced 
automatically in the CI pipeline.