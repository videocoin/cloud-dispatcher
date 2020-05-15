package iam

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Client struct {
	endpoint string
}

func NewClient(endpoint string) (*Client, error) {
	cli := &Client{
		endpoint: endpoint,
	}
	return cli, nil
}

func (c *Client) CreateKey(token string) (*KeyResponse, error) {
	req, err := http.NewRequest("POST", c.endpoint+"/v1/keys", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Add("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()

		bodyErr, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error %d", resp.StatusCode)
		}

		respErr := new(ResponseError)
		err = json.Unmarshal(bodyErr, respErr)
		if err != nil {
			return nil, fmt.Errorf("error %d", resp.StatusCode)
		}

		return nil, errors.New(respErr.Message)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	key := new(KeyResponse)
	err = json.Unmarshal(body, key)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func (c *Client) CreateServiceAccount(token string, clientID string) (*ServiceAccount, error) {
	key, err := c.CreateKey(token)
	if err != nil {
		return nil, err
	}

	pk, err := base64.StdEncoding.DecodeString(key.PrivateKeyData)
	if err != nil {
		return nil, err
	}

	sa := &ServiceAccount{
		Type:         "service_account",
		ClientID:     clientID,
		PrivateKey:   string(pk),
		PrivateKeyID: key.ID,
	}

	return sa, nil
}

func (c *Client) CreateServiceAccountJSON(token string, clientID string) ([]byte, error) {
	sa, err := c.CreateServiceAccount(token, clientID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(sa)
}
