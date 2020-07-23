package service

type Config struct {
	Name    string `envconfig:"-"`
	Version string `envconfig:"-"`

	RPCAddr            string `envconfig:"RPC_ADDR" default:"0.0.0.0:5008"`
	AccountsRPCAddr    string `envconfig:"ACCOUNTS_RPC_ADDR" default:"0.0.0.0:5001"`
	StreamsRPCAddr     string `envconfig:"STREAMS_RPC_ADDR" default:"0.0.0.0:5102"`
	EmitterRPCAddr     string `envconfig:"EMITTER_RPC_ADDR" default:"0.0.0.0:5003"`
	ValidatorRPCAddr   string `envconfig:"VALIDATOR_RPC_ADDR" default:"0.0.0.0:5020"`
	MinersRPCAddr      string `envconfig:"MINERS_RPC_ADDR" default:"0.0.0.0:5011"`
	MediaServerRPCAddr string `envconfig:"MEDIASERVER_RPC_ADDR" default:"0.0.0.0:5090"`
	MetricsAddr        string `envconfig:"METRICS_ADDR" default:"0.0.0.0:15008"`
	BaseInputURL       string `envconfig:"BASE_INPUT_URL" default:""`
	BaseOutputURL      string `envconfig:"BASE_OUTPUT_URL" default:""`
	DBURI              string `envconfig:"DBURI" default:"root:@/videocoin?charset=utf8&parseTime=True&loc=Local"`
	MQURI              string `envconfig:"MQURI" default:"amqp://guest:guest@127.0.0.1:5672"`
	AuthTokenSecret    string `envconfig:"AUTH_TOKEN_SECRET" default:""`
	RPCNodeURL         string `envconfig:"RPC_NODE_URL" default:"https://dev1:D6msEL93LJT5RaPk@rpc.dev.kili.videocoin.network"`
	SyncerURL          string `envconfig:"SYNCER_URL" default:"https://dev.videocoin.network/api/v1/sync"`
	IamEndpoint        string `envconfig:"IAM_ENDPOINT" default:"https://iam.dev.videocoinapis.com"`
	DelegatorUserID    string `envconfig:"DELEGATOR_USER_ID" default:"113a66d2-c75e-4885-4371-2d54bbaf3144"`
	DelegatorToken     string `envconfig:"DELEGATOR_TOKEN" default:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoxLCJleHAiOjE2MTEyNTk3NzIsInN1YiI6IjExM2E2NmQyLWM3NWUtNDg4NS00MzcxLTJkNTRiYmFmMzE0NCJ9.7tKmeJR2LNMSeoziGcApxL9ek9mXbw-Rqb5SOXzaWsc"`
	ModeOnlyInternal   bool   `envconfig:"MODE_ONLY_INTERNAL" default:"false"`
	ModeMinimalVersion string `envconfig:"MODE_MINIMAL_VERSION" default:""`
	StakingManagerAddr string `envconfig:"STAKING_MANAGER_ADDR" default:"0x74feC37C1CEe00F2EA987080D27e370d79cb46dd"`
}
