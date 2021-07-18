# refer to grpc
# https://grpc.io/docs/languages/go/basics/

gen: protoc

protoc:
	protoc --go_out=. \
        --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false \
        ./proto/mr.proto

install:
	#brew install protobuf
	#go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26
	#go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1