package consul

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	consulapi "github.com/hashicorp/consul/api"
)

var (
	ErrAlreadyLocked = errors.New("already locked")
)

type Client struct {
	env string
	cli *consulapi.Client
}

func NewClient(env string, addr string) (*Client, error) {
	cfg := consulapi.DefaultConfig()
	cfg.Address = addr

	consul, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul: failed to create client: %s", err)
	}

	_, err = consul.Agent().Self()
	if err != nil {
		return nil, fmt.Errorf("consul: failed to connect agent: %s", err)
	}

	return &Client{cli: consul, env: env}, nil
}

func (c *Client) Cli() *consulapi.Client {
	return c.cli
}

func (c *Client) GetTranscoderClientIds() ([]string, error) {
	clientIds := []string{}

	pairs, _, err := c.cli.KV().List(fmt.Sprintf("config/%s/services/transcoder/clientids", c.env), nil)
	if err != nil {
		return nil, err
	}

	for _, pair := range pairs {
		clientIds = append(clientIds, string(pair.Value))
	}

	return clientIds, nil
}

func (c *Client) GetTranscoderKeyAndSecret() (consulapi.KVPairs, error) {
	pairs, _, err := c.cli.KV().List(fmt.Sprintf("config/%s/services/transcoder/keys", c.env), nil)
	if err != nil {
		return nil, err
	}

	ksPairs := consulapi.KVPairs{}
	for _, pair := range pairs {
		newPair := pair
		path := strings.Split(newPair.Key, "/")
		keyDecoded, err := base64.RawStdEncoding.DecodeString(path[len(path)-1])
		if err != nil {
			return nil, err
		}
		newPair.Key = string(keyDecoded)
		ksPairs = append(ksPairs, newPair)
	}

	return ksPairs, nil
}
