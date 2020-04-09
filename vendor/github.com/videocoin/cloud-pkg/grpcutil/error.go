package grpcutil

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func IsNotFoundError(err error) bool {
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.NotFound {
			return true
		}
	}

	return false
}
