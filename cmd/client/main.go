package main

import (
	"context"
	"fmt"
	hello_rpc "sharding/rpc/hello/v1"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func concurrent(num int, threadCount int, printCount uint64, fn func(k int)) {
	var wg sync.WaitGroup
	wg.Add(threadCount)

	counter := uint64(0)

	var mut sync.Mutex
	lastPrinted := time.Now()

	numPerThread := num / threadCount

	for i := 0; i < num; i += numPerThread {
		start := i
		end := i + numPerThread
		go func() {
			defer wg.Done()
			for k := start; k < end; k++ {
				fn(k)
				newCounter := atomic.AddUint64(&counter, 1)
				if newCounter%printCount == 0 {
					mut.Lock()
					now := time.Now()
					d := now.Sub(lastPrinted)
					lastPrinted = now
					mut.Unlock()
					fmt.Println("concurrent", printCount, ":", d)
				}
			}
		}()
	}

	wg.Wait()
}

func main() {
	conn, err := grpc.Dial("localhost:7000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := hello_rpc.NewHelloClient(conn)
	ctx := context.Background()
	req := &hello_rpc.IncreaseRequest{
		Counter: 150,
	}

	start := time.Now()
	concurrent(10000, 100, 1000, func(k int) {
		_, err = client.Increase(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				fmt.Println("Code:", st.Code())
				fmt.Println("Message:", st.Message())
			}
			panic(err)
		}
	})
	fmt.Println(time.Since(start))
}
