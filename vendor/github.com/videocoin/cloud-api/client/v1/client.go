package v1

import (
	"context"

	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	dispatcherv1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	profilesv1 "github.com/videocoin/cloud-api/profiles/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"google.golang.org/grpc"
)

type ServiceClient struct {
	Accounts   accountsv1.AccountServiceClient
	Emitter    emitterv1.EmitterServiceClient
	Miners     minersv1.MinersServiceClient
	Profiles   profilesv1.ProfilesServiceClient
	Streams    streamsv1.StreamsServiceClient
	Validator  validatorv1.ValidatorServiceClient
	Dispatcher dispatcherv1.DispatcherServiceClient
}

func NewServiceClientFromEnvconfig(ctx context.Context, config interface{}) (*ServiceClient, error) {
	sc := &ServiceClient{}
	opts := NewDefaultClientDialOption(ctx)

	info := gatherServiceClientInfo(config)
	for _, item := range info {
		switch item.Name {
		case "accounts":
			{
				cli, err := NewAccountsServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Accounts = cli
			}
		case "emitter":
			{
				cli, err := NewEmitterServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Emitter = cli
			}
		case "miners":
			{
				cli, err := NewMinersServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Miners = cli
			}
		case "profiles":
			{
				cli, err := NewProfilesServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Profiles = cli
			}
		case "streams":
			{
				cli, err := NewStreamsServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Streams = cli
			}
		case "validator":
			{
				cli, err := NewValidatorServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Validator = cli
			}
		case "dispatcher":
			{
				cli, err := NewDispatcherServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Dispatcher = cli
			}
		}
	}

	return sc, nil
}

func NewAccountsServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (accountsv1.AccountServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return accountsv1.NewAccountServiceClient(conn), nil
}

func NewEmitterServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (emitterv1.EmitterServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return emitterv1.NewEmitterServiceClient(conn), nil
}

func NewMinersServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (minersv1.MinersServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return minersv1.NewMinersServiceClient(conn), nil
}

func NewProfilesServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (profilesv1.ProfilesServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return profilesv1.NewProfilesServiceClient(conn), nil
}

func NewStreamsServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (streamsv1.StreamsServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return streamsv1.NewStreamsServiceClient(conn), nil
}

func NewValidatorServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (validatorv1.ValidatorServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return validatorv1.NewValidatorServiceClient(conn), nil
}

func NewDispatcherServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (dispatcherv1.DispatcherServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return dispatcherv1.NewDispatcherServiceClient(conn), nil
}
