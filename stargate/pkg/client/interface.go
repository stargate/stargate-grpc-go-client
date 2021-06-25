package client

type StargateClientIFace interface {
	ExecuteQuery(query *Query) (*Response, error)
	ExecuteBatch(batch *Batch) (*Response, error)
}
