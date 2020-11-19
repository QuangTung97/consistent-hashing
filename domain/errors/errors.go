package errors

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error struct {
	RpcCode codes.Code
	Code    string
	Message string
}

var registeredErrors = make(map[string]struct{})

func (err Error) Error() string {
	return fmt.Sprintf("code: %s, message: %s", err.Code, err.Message)
}

func checkCode(code string) codes.Code {
	if len(code) != 5 {
		msg := fmt.Sprintf("Code length must be 5 for %q", code)
		panic(msg)
	}

	rpc := code[0:2]
	rpcCode, err := strconv.ParseUint(rpc, 10, 32)
	if err != nil {
		panic(err)
	}

	if rpcCode < 1 || rpcCode > 16 {
		msg := fmt.Sprintf("Invalid gRPC code for %q", code)
		panic(msg)
	}

	_, err = strconv.ParseUint(code[2:], 10, 32)
	if err != nil {
		panic(err)
	}

	return codes.Code(rpcCode)
}

func New(code, msg string) error {
	rpcCode := checkCode(code)

	_, existed := registeredErrors[code]
	if existed {
		msg := fmt.Sprintf("Error code %q already existed", code)
		panic(msg)
	}
	registeredErrors[code] = struct{}{}

	return Error{
		RpcCode: rpcCode,
		Code:    code,
		Message: msg,
	}
}

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			domainErr, ok := err.(Error)
			if !ok {
				return resp, err
			}
			st := status.New(domainErr.RpcCode, domainErr.Error())
			return resp, st.Err()
		}
		return resp, nil
	}
}
