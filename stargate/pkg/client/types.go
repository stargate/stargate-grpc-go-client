package client

type TypeSpec interface {
	isTypeSpec()
}

type TypeSpecBasic struct {
	Basic Basic
}

func (TypeSpecBasic) isTypeSpec() {}

type Basic int32

const (
	CUSTOM    Basic = 0
	ASCII     Basic = 1
	BIGINT    Basic = 2
	BLOB      Basic = 3
	BOOLEAN   Basic = 4
	COUNTER   Basic = 5
	DECIMAL   Basic = 6
	DOUBLE    Basic = 7
	FLOAT     Basic = 8
	INT       Basic = 9
	TEXT      Basic = 10
	TIMESTAMP Basic = 11
	UUID      Basic = 12
	VARCHAR   Basic = 13
	VARINT    Basic = 14
	TIMEUUID  Basic = 15
	INET      Basic = 16
	DATE      Basic = 17
	TIME      Basic = 18
	SMALLINT  Basic = 19
	TINYINT   Basic = 20
)

type TypeSpecMap struct {
	Key   *TypeSpec
	Value *TypeSpec
}

func (TypeSpecMap) isTypeSpec() {}

type TypeSpecList struct {
	Element *TypeSpec
}

func (TypeSpecList) isTypeSpec() {}

type TypeSpecSet struct {
	Element *TypeSpec
}

func (TypeSpecSet) isTypeSpec() {}

type TypeSpecUdt struct {
	Fields map[string]*TypeSpec
}

func (TypeSpecUdt) isTypeSpec() {}

type TypeSpecTuple struct {
	Elements []*TypeSpec
}

func (TypeSpecTuple) isTypeSpec() {}
