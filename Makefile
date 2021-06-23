export GO111MODULE = on
export GOBIN = $(PWD)/.bin
export PROTOC = protoc
#export PATH = "$(PATH):$(GOBIN)"

.PHONY: all install-plugins proto

all: proto

install-plugins:
	go get google.golang.org/protobuf/cmd/protoc-gen-go \
         google.golang.org/grpc/cmd/protoc-gen-go-grpc

PROTO_FILES = $(wildcard proto/*.proto)
PROTO_GO_FILES = $(foreach name, $(basename $(notdir $(PROTO_FILES))), stargate/proto/$(name)/$(name).pb.go)

proto: $(PROTO_GO_FILES)

$(PROTO_GO_FILES): install-plugins $(PROTO_FILES)
	for name in $(PROTO_FILES); do \
		$(PROTOC) -Iproto --plugin=.bin/protoc-gen-go --plugin=.bin/protoc-gen-go-grpc --go_out=stargate/pkg/proto --go_opt=paths=source_relative --go-grpc_out=stargate/pkg/proto --go-grpc_opt=paths=source_relative $${name}; \
	done

clean:
	rm -f stargate/proto/*.pb.go
