package errors

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type domainErr struct {
	rpcCode codes.Code
	code    string
	message string
}

var registeredErrors = make(map[string]struct{})

func (err domainErr) Error() string {
	return fmt.Sprintf("code: %s, message: %s", err.code, err.message)
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

// New creates an domain error
func New(code, msg string) error {
	rpcCode := checkCode(code)

	_, existed := registeredErrors[code]
	if existed {
		msg := fmt.Sprintf("Error code %q already existed", code)
		panic(msg)
	}
	registeredErrors[code] = struct{}{}

	return domainErr{
		rpcCode: rpcCode,
		code:    code,
		message: msg,
	}
}

// UnaryServerInterceptor creates a server interceptor
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			domainErr, ok := err.(domainErr)
			if !ok {
				return resp, err
			}
			st := status.New(domainErr.rpcCode, domainErr.Error())
			return resp, st.Err()
		}
		return resp, nil
	}
}
