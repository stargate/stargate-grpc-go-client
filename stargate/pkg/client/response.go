package client

type Response struct {
	ResultSet *ResultSet
	TracingId []byte
	Warnings  []string
}

type ResultSet struct {
	Columns     []*ColumnSpec
	Rows        []*Row
	PageSize    int32
	PagingState []byte
}

type ColumnSpec struct {
	TypeSpec TypeSpec
	Name     string
}

type Row struct {
	Values []*Value
}
