package rpc

import (
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrRpcInternal         = grpc.Errorf(codes.Internal, "Oops. Something went wrong! Sorry. We've let our engineers know.")
	ErrRpcUnauthenticated  = grpc.Errorf(codes.Unauthenticated, "Unauthenticated")
	ErrRpcPermissionDenied = grpc.Errorf(codes.PermissionDenied, "Permission Denied")
	ErrRpcNotFound         = grpc.Errorf(codes.NotFound, "Not Found")
)

func NewRpcValidationError(verr proto.Message) error {
	s, _ := status.New(codes.InvalidArgument, "").WithDetails(verr)
	return s.Err()
}
