package service

import (
	"github.com/sirupsen/logrus"
)

type Config struct {
	Name    string `envconfig:"-"`
	Version string `envconfig:"-"`

	RPCAddr          string `default:"0.0.0.0:5008" envconfig:"RPC_ADDR"`
	AccountsRPCAddr  string `default:"0.0.0.0:5001" envconfig:"ACCOUNTS_RPC_ADDR"`
	StreamsRPCAddr   string `default:"0.0.0.0:5102" envconfig:"STREAMS_RPC_ADDR"`
	EmitterRPCAddr   string `default:"0.0.0.0:5003" envconfig:"EMITTER_RPC_ADDR"`
	ProfilesRPCAddr  string `default:"0.0.0.0:5004" envconfig:"PROFILES_RPC_ADDR"`
	ValidatorRPCAddr string `default:"0.0.0.0:5020" envconfig:"VALIDATOR_RPC_ADDR"`
	SyncerRPCAddr    string `default:"0.0.0.0:5021" envconfig:"SYNCER_RPC_ADDR"`
	MinersRPCAddr    string `default:"0.0.0.0:5011" envconfig:"MINERS_RPC_ADDR"`
	MetricsAddr      string `default:"0.0.0.0:15008" envconfig:"METRICS_ADDR"`
	ConsulAddr       string `default:"127.0.0.1:8500" envconfig:"CONSUL_ADDR"`
	Env				 string `default:"dev"`

	DBURI string `default:"root:@/videocoin?charset=utf8&parseTime=True&loc=Local" envconfig:"DBURI"`
	MQURI string `default:"amqp://guest:guest@127.0.0.1:5672" envconfig:"MQURI"`

	AuthTokenSecret string `default:"" envconfig:"AUTH_TOKEN_SECRET"`

	BaseInputURL  string `default:"" envconfig:"BASE_INPUT_URL"`
	BaseOutputURL string `default:"" envconfig:"BASE_OUTPUT_URL"`

	Logger *logrus.Entry `envconfig:"-"`
}
