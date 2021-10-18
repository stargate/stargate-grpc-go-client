package auth

import (
	"context"

	"google.golang.org/grpc/credentials"
)

type staticTokenProvider struct {
	token                    string
	requireTransportSecurity bool
}

// NewStaticTokenProvider will take the provided token and use it to populate the `x-cassandra-token` header for all requests.
func NewStaticTokenProvider(token string) credentials.PerRPCCredentials {
	return staticTokenProvider{
		token:                    token,
		requireTransportSecurity: true,
	}
}

// NewStaticTokenProviderUnsafe is identical to NewStaticTokenProvider except that it will set requireTransportSecurity
// to false for environments where transport security it not in use.
func NewStaticTokenProviderUnsafe(token string) credentials.PerRPCCredentials {
	return staticTokenProvider{
		token:                    token,
		requireTransportSecurity: false,
	}
}

func (s staticTokenProvider) RequireTransportSecurity() bool {
	return s.requireTransportSecurity
}

func (s staticTokenProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"x-cassandra-token": s.token}, nil
}
