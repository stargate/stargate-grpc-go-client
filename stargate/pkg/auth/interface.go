package auth

import "context"

type AuthProvider interface {
	GetToken(ctx context.Context) (string, error)
}
