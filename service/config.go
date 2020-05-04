package service

type Config struct {
	Name    string `envconfig:"-"`
	Version string `envconfig:"-"`

	RPCAddr          string `envconfig:"RPC_ADDR" default:"0.0.0.0:5008"`
	AccountsRPCAddr  string `envconfig:"ACCOUNTS_RPC_ADDR" default:"0.0.0.0:5001"`
	StreamsRPCAddr   string `envconfig:"STREAMS_RPC_ADDR" default:"0.0.0.0:5102"`
	EmitterRPCAddr   string `envconfig:"EMITTER_RPC_ADDR" default:"0.0.0.0:5003"`
	ProfilesRPCAddr  string `envconfig:"PROFILES_RPC_ADDR" default:"0.0.0.0:5004"`
	ValidatorRPCAddr string `envconfig:"VALIDATOR_RPC_ADDR" default:"0.0.0.0:5020"`
	MinersRPCAddr    string `envconfig:"MINERS_RPC_ADDR" default:"0.0.0.0:5011"`
	MetricsAddr      string `envconfig:"METRICS_ADDR" default:"0.0.0.0:15008"`
	BaseInputURL     string `envconfig:"BASE_INPUT_URL" default:""`
	BaseOutputURL    string `envconfig:"BASE_OUTPUT_URL" default:""`
	DBURI            string `envconfig:"DBURI" default:"root:@/videocoin?charset=utf8&parseTime=True&loc=Local"`
	MQURI            string `envconfig:"MQURI" default:"amqp://guest:guest@127.0.0.1:5672"`
	AuthTokenSecret  string `envconfig:"AUTH_TOKEN_SECRET" default:""`
	RPCNodeURL       string `envconfig:"RPC_NODE_URL" default:"https://dev1:D6msEL93LJT5RaPk@rpc.dev.kili.videocoin.network"`
	SyncerURL        string `envconfig:"SYNCER_URL" default:"https://dev.videocoin.network/api/v1/sync"`
}
