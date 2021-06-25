package client

import (
	log "github.com/sirupsen/logrus"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

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
	Key   TypeSpec
	Value TypeSpec
}

func (TypeSpecMap) isTypeSpec() {}

type TypeSpecList struct {
	Element TypeSpec
}

func (TypeSpecList) isTypeSpec() {}

type TypeSpecSet struct {
	Element TypeSpec
}

func (TypeSpecSet) isTypeSpec() {}

type TypeSpecUdt struct {
	Fields map[string]TypeSpec
}

func (TypeSpecUdt) isTypeSpec() {}

type TypeSpecTuple struct {
	Elements []TypeSpec
}

func (TypeSpecTuple) isTypeSpec() {}

func mapTypeSpec(spec *pb.TypeSpec) TypeSpec {
	switch spec.GetSpec().(type) {
	case *pb.TypeSpec_Basic_:
		return mapBasicType(spec)
	case *pb.TypeSpec_Map_:
		log.Debug("map")
		return TypeSpecMap{
			Key:   mapTypeSpec(spec.GetMap().Key),
			Value: mapTypeSpec(spec.GetMap().Value),
		}
	case *pb.TypeSpec_List_:
		log.Debug("list")
		return TypeSpecList{Element: mapTypeSpec(spec.GetList().Element)}
	case *pb.TypeSpec_Set_:
		log.Debug("set")
		return TypeSpecSet{Element: mapTypeSpec(spec.GetSet().Element)}
	case *pb.TypeSpec_Udt_:
		log.Debug("udt")
		fields := map[string]TypeSpec{}
		for s, typeSpec := range spec.GetUdt().Fields {
			fields[s] = mapTypeSpec(typeSpec)
		}
		return TypeSpecUdt{Fields: fields}
	case *pb.TypeSpec_Tuple_:
		log.Debug("tuple")
		var elements []TypeSpec
		for _, element := range spec.GetTuple().Elements {
			elements = append(elements, mapTypeSpec(element))
		}
		return TypeSpecTuple{Elements: elements}
	}
	return TypeSpecBasic{VARCHAR}
}

func mapBasicType(spec *pb.TypeSpec) TypeSpec {
	switch spec.GetBasic() {
	case pb.TypeSpec_CUSTOM:
		log.Debug("custom")

		return TypeSpecBasic{CUSTOM}
	case pb.TypeSpec_ASCII:
		log.Debug("ascii")

		return TypeSpecBasic{ASCII}
	case pb.TypeSpec_BIGINT:
		log.Debug("bigint")

		return TypeSpecBasic{BIGINT}
	case pb.TypeSpec_BLOB:
		log.Debug("blob")

		return TypeSpecBasic{BLOB}
	case pb.TypeSpec_BOOLEAN:
		log.Debug("boolean")

		return TypeSpecBasic{BOOLEAN}
	case pb.TypeSpec_COUNTER:
		log.Debug("counter")

		return TypeSpecBasic{COUNTER}
	case pb.TypeSpec_DECIMAL:
		log.Debug("decimal")

		return TypeSpecBasic{DECIMAL}
	case pb.TypeSpec_DOUBLE:
		log.Debug("double")

		return TypeSpecBasic{DOUBLE}
	case pb.TypeSpec_FLOAT:
		log.Debug("float")

		return TypeSpecBasic{FLOAT}
	case pb.TypeSpec_INT:
		log.Debug("int")

		return TypeSpecBasic{INT}
	case pb.TypeSpec_TEXT:
		log.Debug("text")

		return TypeSpecBasic{TEXT}
	case pb.TypeSpec_TIMESTAMP:
		log.Debug("timestamp")

		return TypeSpecBasic{TIMESTAMP}
	case pb.TypeSpec_UUID:
		log.Debug("uuid")

		return TypeSpecBasic{UUID}
	case pb.TypeSpec_VARCHAR:
		log.Debug("varchar")

		return TypeSpecBasic{VARCHAR}
	case pb.TypeSpec_VARINT:
		log.Debug("varint")

		return TypeSpecBasic{VARINT}
	case pb.TypeSpec_TIMEUUID:
		log.Debug("timeuuid")

		return TypeSpecBasic{TIMEUUID}
	case pb.TypeSpec_INET:
		log.Debug("inet")

		return TypeSpecBasic{INET}
	case pb.TypeSpec_DATE:
		log.Debug("date")

		return TypeSpecBasic{DATE}
	case pb.TypeSpec_TIME:
		log.Debug("time")

		return TypeSpecBasic{TIME}
	case pb.TypeSpec_SMALLINT:
		log.Debug("smallint")

		return TypeSpecBasic{SMALLINT}
	case pb.TypeSpec_TINYINT:
		log.Debug("tinyint")

		return TypeSpecBasic{TINYINT}
	}

	return TypeSpecBasic{VARCHAR}
}