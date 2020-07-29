package v1

import (
	"context"

	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	billingv1 "github.com/videocoin/cloud-api/billing/private/v1"
	dispatcherv1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	mediaserverv1 "github.com/videocoin/cloud-api/mediaserver/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	usersv1 "github.com/videocoin/cloud-api/users/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"google.golang.org/grpc"
)

type ServiceClient struct {
	Accounts    accountsv1.AccountServiceClient
	Billing     billingv1.BillingServiceClient
	Dispatcher  dispatcherv1.DispatcherServiceClient
	Emitter     emitterv1.EmitterServiceClient
	MediaServer mediaserverv1.MediaServerServiceClient
	Miners      minersv1.MinersServiceClient
	Streams     streamsv1.StreamsServiceClient
	Users       usersv1.UserServiceClient
	Validator   validatorv1.ValidatorServiceClient
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
		case "billing":
			{
				cli, err := NewBillingServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Billing = cli
			}
		case "dispatcher":
			{
				cli, err := NewDispatcherServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Dispatcher = cli
			}
		case "emitter":
			{
				cli, err := NewEmitterServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Emitter = cli
			}
		case "mediaserver":
			{
				cli, err := NewMediaServerServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.MediaServer = cli
			}
		case "miners":
			{
				cli, err := NewMinersServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Miners = cli
			}
		case "streams":
			{
				cli, err := NewStreamsServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Streams = cli
			}
		case "users":
			{
				cli, err := NewUsersServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Users = cli
			}
		case "validator":
			{
				cli, err := NewValidatorServiceClient(ctx, item.Addr, opts...)
				if err != nil {
					return nil, err
				}
				sc.Validator = cli
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

func NewBillingServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (billingv1.BillingServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return billingv1.NewBillingServiceClient(conn), nil
}

func NewDispatcherServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (dispatcherv1.DispatcherServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return dispatcherv1.NewDispatcherServiceClient(conn), nil
}

func NewEmitterServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (emitterv1.EmitterServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return emitterv1.NewEmitterServiceClient(conn), nil
}

func NewMediaServerServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (mediaserverv1.MediaServerServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return mediaserverv1.NewMediaServerServiceClient(conn), nil
}

func NewMinersServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (minersv1.MinersServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return minersv1.NewMinersServiceClient(conn), nil
}

func NewStreamsServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (streamsv1.StreamsServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return streamsv1.NewStreamsServiceClient(conn), nil
}

func NewUsersServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (usersv1.UserServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return usersv1.NewUserServiceClient(conn), nil
}

func NewValidatorServiceClient(ctx context.Context, addr string, opts ...grpc.DialOption) (validatorv1.ValidatorServiceClient, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return validatorv1.NewValidatorServiceClient(conn), nil
}
