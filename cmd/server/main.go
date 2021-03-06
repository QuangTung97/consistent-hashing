package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sharding/domain/errors"
	service "sharding/service"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"google.golang.org/grpc"

	_ "github.com/go-sql-driver/mysql"
)

func deciderAllMethods(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
	return true
}

func initServer(logger *zap.Logger) (*grpc.Server, *service.Root) {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_prometheus.UnaryServerInterceptor,
				grpc_zap.UnaryServerInterceptor(logger),
				// grpc_zap.PayloadUnaryServerInterceptor(logger, decider),
				errors.UnaryServerInterceptor(),
			),
		),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_prometheus.StreamServerInterceptor,
				grpc_zap.StreamServerInterceptor(logger),
				// grpc_zap.PayloadStreamServerInterceptor(logger, decider),
			),
		),
	)

	root := service.InitRoot(server, logger)

	grpc_prometheus.Register(server)
	grpc_prometheus.EnableHandlingTimeHistogram()

	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{OrigName: true}),
	)

	opts := []grpc.DialOption{grpc.WithInsecure()}

	service.InitGatewayEndpoints(mux, root.GetNodeConfig().ToAddress(), opts)

	http.Handle("/api/", mux)
	http.Handle("/metrics", promhttp.Handler())

	return server, root
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	server, root := initServer(logger)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, os.Kill)

	lis, err := net.Listen("tcp", root.GetNodeConfig().ToListenAddr())
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		err := server.Serve(lis)
		if err != nil {
			logger.Error("serve", zap.Error(err))
		}
	}()

	httpServer := http.Server{
		Addr: root.GetNodeConfig().ToGatewayListenAddr(),
	}

	go func() {
		defer wg.Done()

		err := httpServer.ListenAndServe()
		if err == http.ErrServerClosed {
			return
		}
		if err != nil {
			logger.Error("httpServer Listen", zap.Error(err))
		}
	}()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer wg.Done()

		root.Run(ctx)
	}()

	signal := <-exit
	fmt.Println("SIGNAL", signal)
	cancel()

	server.GracefulStop()

	err = httpServer.Shutdown(ctx)
	if err != nil {
		logger.Error("httpServer Shutdown", zap.Error(err))
	}

	wg.Wait()

	fmt.Println("Stop successfully")
}
