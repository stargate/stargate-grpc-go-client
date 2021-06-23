package client

type Value struct {
	Inner ValueInner
}

type ValueInner interface {
	isValue()
}

type ValueString struct {
	String string
}

func (ValueString) isValue() {}

type ValueCollection struct {
	Collection *Collection
}

func (ValueCollection) isValue() {}

type Collection struct {
	Elements []*Value
}

type ValueUdt struct {
	Udt *UdtValue
}

func (ValueUdt) isValue() {}

type UdtValue struct {
	Fields map[string]*Value
}

