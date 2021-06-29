package auth

type AuthProviderIFace interface {
	GetToken() (string, error)
}
