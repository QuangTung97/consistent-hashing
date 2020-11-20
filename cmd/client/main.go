package main

import (
	"context"
	"fmt"
	hello_rpc "sharding/rpc/hello/v1"

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

	req := &hello_rpc.IncreaseRequest{
		Counter: 100,
	}

	_, err = client.Increase(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			fmt.Println("Code:", st.Code())
			fmt.Println("Message:", st.Message())
		}
		panic(err)
	}
}
