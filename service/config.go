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
	RPCNodeURL         string `envconfig:"RPC_NODE_URL"`
	SyncerURL          string `envconfig:"SYNCER_URL" default:"https://dev.videocoin.network/api/v1/sync"`
	ModeOnlyInternal   bool   `envconfig:"MODE_ONLY_INTERNAL" default:"false"`
	ModeMinimalVersion string `envconfig:"MODE_MINIMAL_VERSION" default:""`
	StakingManagerAddr string `envconfig:"STAKING_MANAGER_ADDR" default:"0x74feC37C1CEe00F2EA987080D27e370d79cb46dd"`
}
