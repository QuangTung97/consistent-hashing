package main

import (
	"context"
	"fmt"
	hello_rpc "sharding/rpc/hello/v1"
	"time"

	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.Dial("localhost:5000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := hello_rpc.NewHelloClient(conn)

	ctx := context.Background()

	createdAt, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		panic(err)
	}

	req := &hello_rpc.HelloRequest{
		Name:      "Quang Tung",
		CreatedAt: createdAt,
	}

	_, err = client.Hello(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			fmt.Println("Code:", st.Code())
			fmt.Println("Message:", st.Message())
		}
		panic(err)
	}
}
