package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/credentials"
)

type tableBasedTokenProvider struct {
	client   *client
	username string
	password string
	requireTransportSecurity bool
}

type client struct {
	serviceURL string
	httpClient *http.Client
}

type authResponse struct {
	AuthToken string `json:"authToken"`
}

type authRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// NewTableBasedTokenProvider creates a token provider intended to be used with Stargate's table based token authentication mechanism. This
// function will generate a token by making a request to the provided Stargate auth-api URL and populating the `x-cassandra-token` header
// with the returned token.
func NewTableBasedTokenProvider(serviceURL, username, password string) credentials.PerRPCCredentials {
	return tableBasedTokenProvider{
		client:   getClient(serviceURL),
		username: username,
		password: password,
		requireTransportSecurity: true,
	}
}

// NewTableBasedTokenProviderUnsafe is identical to NewTableBasedTokenProvider except that it will set requireTransportSecurity
// to false for environments where transport security it not in use.
func NewTableBasedTokenProviderUnsafe(serviceURL, username, password string) credentials.PerRPCCredentials {
	return tableBasedTokenProvider{
		client:   getClient(serviceURL),
		username: username,
		password: password,
		requireTransportSecurity: false,
	}
}

func (t tableBasedTokenProvider) RequireTransportSecurity() bool {
	return t.requireTransportSecurity
}

func (t tableBasedTokenProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := t.getToken(ctx)
	if err != nil {
		log.WithError(err).Error("Failed to get auth token")
		return nil, fmt.Errorf("failed to get auth token: %v", err)
	}
	return map[string]string{"x-cassandra-token": token}, nil
}

// TODO: [doug] Figure out how to cache
func (t tableBasedTokenProvider) getToken(ctx context.Context) (string, error) {
	authReq := authRequest{
		Username: t.username,
		Password: t.password,
	}
	jsonString, err := json.Marshal(authReq)
	if err != nil {
		log.Errorf("error marshalling request: %v", err)
		return "", fmt.Errorf("error marshalling request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.client.serviceURL, bytes.NewBuffer(jsonString))
	if err != nil {
		log.Errorf("error creating request: %v", err)
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := t.client.httpClient.Do(req)
	if err != nil {
		log.Errorf("error calling auth service: %v", err)
		return "", fmt.Errorf("error calling auth service: %v", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Warnf("unable to close response body: %v", err)
		}
	}()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("error reading response body: %v", err)
		return "", fmt.Errorf("error reading response body: %v", err)
	}

	ar := authResponse{}
	err = json.Unmarshal(body, &ar)
	if err != nil {
		log.Errorf("error unmarshalling response body: %v", err)
		return "", fmt.Errorf("error unmarshalling response body: %v", err)
	}

	return ar.AuthToken, nil
}

func getClient(serviceURL string) *client {
	return &client{
		serviceURL: serviceURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}
