package iam

type KeyResponse struct {
	ID             string `json:"id"`
	PrivateKeyData string `json:"private_key_data"`
}

type ServiceAccount struct {
	Type         string `json:"type"`
	ClientID     string `json:"client_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
}
