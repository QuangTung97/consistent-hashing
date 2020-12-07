.PHONY: all

PROTO_DIR := proto
RPC_DIR := rpc

CURRENT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

GRPC_GATEWAY := $(shell go list -m -f "{{.Dir}}" github.com/grpc-ecosystem/grpc-gateway)
GOGOPROTOBUF := $(shell go list -m -f "{{.Dir}}" github.com/gogo/protobuf)

M_OPTIONS := Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,$\
	Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,$\
	Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,$\
	Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,$\
	Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types

PROTOC_INCLUDES := .:$\
	${CURRENT_DIR}/${PROTO_DIR}:$\
	${GRPC_GATEWAY}/third_party/googleapis:$\
	${GOGOPROTOBUF}/protobuf

define generate
	mkdir -p ${RPC_DIR}/$(1) && \
		cd ${PROTO_DIR}/$(1) && \
		protoc -I${PROTOC_INCLUDES} \
			--gofast_out=paths=source_relative,plugins=grpc,${M_OPTIONS}:${CURRENT_DIR}/${RPC_DIR}/$(1) \
			--grpc-gateway_out=logtostderr=true,paths=source_relative:${CURRENT_DIR}/${RPC_DIR}/$(1) \
			$(2)
endef

all:
	go build -o server cmd/server/main.go
	go build -o client cmd/client/main.go
	go build -o proxy cmd/proxy/main.go

gen:
	rm -rf rpc
	$(call generate,hello/v1/,hello.proto)

install-tools:
	go install github.com/gogo/protobuf/protoc-gen-gofast
	go install github.com/fzipp/gocyclo
	go install github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	go install github.com/kisielk/errcheck
	go install golang.org/x/lint/golint

lint:
	go fmt
	golint ./...
	go vet ./...
	errcheck ./...
	gocyclo -over 25 .

test:
	go test -v ./...
