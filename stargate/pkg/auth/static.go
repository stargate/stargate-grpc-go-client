package auth

import (
	"context"

	"google.golang.org/grpc/credentials"
)

type staticTokenProvider struct {
	token string
}

// NewStaticTokenProvider will take the provided token and use it to populate the `x-cassandra-token` header for all requests.
func NewStaticTokenProvider(token string) credentials.PerRPCCredentials {
	return staticTokenProvider{
		token: token,
	}
}

func (s staticTokenProvider) RequireTransportSecurity() bool {
	return false
}

func (s staticTokenProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"x-cassandra-token": s.token}, nil
}
