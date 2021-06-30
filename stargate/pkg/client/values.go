package client

import (
	log "github.com/sirupsen/logrus"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

type Value struct {
	Inner ValueInner
}

type ValueInner interface {
	isValue()
	toProtoInner() pb.Value
}

type ValueString struct {
	String string
}

func (ValueString) isValue() {}

func (v ValueString) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_String_{String_: v.String}}
}

type ValueInt struct {
	// CQL types: tinyint, smallint, int, bigint, counter, timestamp
	Int int64
}

func (ValueInt) isValue() {}

func (v ValueInt) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Int{
		Int: v.Int,
	}}
}

type ValueFloat struct {
	// CQL types: float
	Float float32
}

func (ValueFloat) isValue() {}

func (v ValueFloat) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Float{
		Float: v.Float,
	}}
}

type ValueDouble struct {
	// CQL types: double
	Double float64
}

func (ValueDouble) isValue() {}

func (v ValueDouble) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Double{
		Double: v.Double,
	}}
}

type ValueBoolean struct {
	Boolean bool
}

func (ValueBoolean) isValue() {}

func (v ValueBoolean) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Boolean{
		Boolean: v.Boolean,
	}}
}

type ValueBytes struct {
	// CQL types: blob, inet, custom
	Bytes []byte
}

func (ValueBytes) isValue() {}

func (v ValueBytes) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Bytes{
		Bytes: v.Bytes,
	}}
}

type ValueUUID struct {
	// CQL types: uuid, timeuuid
	UUID *Uuid
}

type Uuid struct {
	Msb uint64
	Lsb uint64
}

func (ValueUUID) isValue() {}

func (v ValueUUID) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Uuid{
		Uuid: &pb.Uuid{
			Msb: v.UUID.Msb,
			Lsb: v.UUID.Lsb,
		},
	}}
}

type ValueDate struct {
	// CQL types: date
	// An unsigned integer representing days with Unix epoch (January, 1 1970) at 2^31.
	// Examples:
	// 0:    -5877641-06-23
	// 2^31: 1970-1-1
	// 2^32: 5881580-07-11
	Date uint32
}

func (ValueDate) isValue() {}

func (v ValueDate) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Date{
		Date: v.Date,
	}}
}

type ValueTime struct {
	// CQL types: time
	// An unsigned integer representing the number of nanoseconds since midnight. Valid values are
	// in the range 0 to 86399999999999 (inclusive).
	Time uint64
}

func (ValueTime) isValue() {}

func (v ValueTime) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Time{
		Time: v.Time,
	}}
}

type ValueCollection struct {
	Collection *Collection
}

func (ValueCollection) isValue() {}

func (v ValueCollection) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Collection{
		Collection: v.Collection.toProto(),
	}}
}

type Collection struct {
	Elements []*Value
}

func (c Collection) toProto() *pb.Collection {
	var elements []*pb.Value
	for _, element := range c.Elements {
		elements = append(elements, &pb.Value{Inner: element.Inner.toProtoInner().Inner})
	}
	return &pb.Collection{
		Elements: elements,
	}
}

type ValueUdt struct {
	UDT *UdtValue
}

func (ValueUdt) isValue() {}

func (v ValueUdt) toProtoInner() pb.Value {
	return pb.Value{Inner: &pb.Value_Udt{
		Udt: v.UDT.toProto(),
	}}
}

type UdtValue struct {
	Fields map[string]*Value
}

func (v UdtValue) toProto() *pb.UdtValue {
	fields := map[string]*pb.Value{}
	for key, field := range v.Fields {
		fields[key] = &pb.Value{Inner: field.Inner.toProtoInner().Inner}
	}

	return &pb.UdtValue{
		Fields: fields,
	}
}

func translateType(spec *pb.TypeSpec, value *pb.Value) *Value {
	if value.GetNull() != nil {
		log.WithField("value", value.GetInner()).Debug("null value")
		return nil
	}

	switch spec.GetSpec().(type) {
	case *pb.TypeSpec_Basic_:
		return translateBasicType(spec, value)
	case *pb.TypeSpec_Map_:
		log.WithField("value", value.GetCollection()).Debug("map")
		var elements []*Value

		for i := 0; i < len(value.GetCollection().Elements)-1; i++ {
			elements = append(elements, translateType(spec.GetMap().Key, value.GetCollection().Elements[i]))
			elements = append(elements, translateType(spec.GetMap().Value, value.GetCollection().Elements[i+1]))
		}
		return &Value{
			Inner: ValueCollection{
				Collection: &Collection{Elements: elements},
			},
		}
	case *pb.TypeSpec_List_:
		log.WithField("value", value.GetCollection()).Debug("list")
		var elements []*Value
		for i := range value.GetCollection().Elements {
			elements = append(elements, translateType(spec.GetList().Element, value.GetCollection().Elements[i]))
		}
		return &Value{
			Inner: ValueCollection{
				Collection: &Collection{Elements: elements},
			},
		}
	case *pb.TypeSpec_Set_:
		log.WithField("value", value.GetCollection()).Debug("set")
		var elements []*Value
		for _, element := range value.GetCollection().Elements {
			elements = append(elements, translateType(spec.GetSet().Element, element))
		}
		return &Value{
			Inner: ValueCollection{
				Collection: &Collection{Elements: elements},
			},
		}
	case *pb.TypeSpec_Udt_:
		log.WithField("value", value.GetUdt()).Debug("udt")
		fields := map[string]*Value{}
		for key, val := range value.GetUdt().Fields {
			fields[key] = translateType(spec.GetUdt().Fields[key], val)
		}

		return &Value{
			Inner: ValueUdt{
				UDT: &UdtValue{
					Fields: fields,
				},
			},
		}
	case *pb.TypeSpec_Tuple_:
		log.WithField("value", value.GetCollection()).Debug("tuple")
		var elements []*Value
		numElements := len(spec.GetTuple().Elements)
		for i := 0; i <= len(value.GetCollection().Elements)-numElements; i++ {
			for j, typeSpec := range spec.GetTuple().Elements {
				elements = append(elements, translateType(typeSpec, value.GetCollection().Elements[i+j]))
			}
		}
		return &Value{
			Inner: ValueCollection{
				Collection: &Collection{Elements: elements},
			},
		}
	}

	return nil
}

func translateBasicType(spec *pb.TypeSpec, value *pb.Value) *Value {
	switch spec.GetBasic() {
	case pb.TypeSpec_CUSTOM:
		log.WithField("value", value.GetBytes()).Debug("custom")

		return &Value{
			Inner: ValueBytes{
				Bytes: value.GetBytes(),
			},
		}
	case pb.TypeSpec_ASCII:
		log.WithField("value", value.GetString_()).Debug("ascii")

		return &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	case pb.TypeSpec_BIGINT:
		log.WithField("value", value.GetInt()).Debug("bigint")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	case pb.TypeSpec_BLOB:
		log.WithField("value", value.GetBytes()).Debug("blob")

		return &Value{
			Inner: ValueBytes{
				Bytes: value.GetBytes(),
			},
		}
	case pb.TypeSpec_BOOLEAN:
		log.WithField("value", value.GetBoolean()).Debug("boolean")

		return &Value{
			Inner: ValueBoolean{Boolean: value.GetBoolean()},
		}
	case pb.TypeSpec_COUNTER:
		log.WithField("value", value.GetInt()).Debug("counter")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	case pb.TypeSpec_DECIMAL:
		log.Debug("decimal")

		// Not currently supported
	case pb.TypeSpec_DOUBLE:
		log.WithField("value", value.GetDouble()).Debug("double")

		return &Value{
			Inner: ValueDouble{
				Double: value.GetDouble(),
			},
		}
	case pb.TypeSpec_FLOAT:
		log.WithField("value", value.GetFloat()).Debug("float")

		return &Value{
			Inner: ValueFloat{
				Float: value.GetFloat(),
			},
		}
	case pb.TypeSpec_INT:
		log.WithField("value", value.GetInt()).Debug("int")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	case pb.TypeSpec_TEXT:
		log.WithField("value", value.GetString_()).Debug("text")

		return &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	case pb.TypeSpec_TIMESTAMP:
		log.WithField("value", value.GetInt()).Debug("timestamp")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	case pb.TypeSpec_UUID:
		log.WithField("value", value.GetString_()).Debug("uuid")

		return &Value{
			Inner: ValueUUID{UUID: &Uuid{
				Msb: value.GetUuid().Msb,
				Lsb: value.GetUuid().Lsb,
			}},
		}
	case pb.TypeSpec_VARCHAR:
		log.WithField("value", value.GetString_()).Debug("varchar")

		return &Value{
			Inner: ValueString{
				String: value.GetString_(),
			},
		}
	case pb.TypeSpec_VARINT:
		log.Debug("varint")
		// Not currently supported
	case pb.TypeSpec_TIMEUUID:
		log.WithField("value", value.GetString_()).Debug("timeuuid")

		return &Value{
			Inner: ValueUUID{UUID: &Uuid{
				Msb: value.GetUuid().Msb,
				Lsb: value.GetUuid().Lsb,
			}},
		}
	case pb.TypeSpec_INET:
		log.WithField("value", value.GetBytes()).Debug("inet")

		return &Value{
			Inner: ValueBytes{
				Bytes: value.GetBytes(),
			},
		}
	case pb.TypeSpec_DATE:
		log.WithField("value", value.GetString_()).Debug("date")

		return &Value{
			Inner: ValueDate{Date: value.GetDate()},
		}
	case pb.TypeSpec_TIME:
		log.WithField("value", value.GetTime()).Debug("time")

		return &Value{
			Inner: ValueTime{
				Time: value.GetTime(),
			},
		}
	case pb.TypeSpec_SMALLINT:
		log.WithField("value", value.GetInt()).Debug("smallint")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	case pb.TypeSpec_TINYINT:
		log.WithField("value", value.GetInt()).Debug("tinyint")

		return &Value{
			Inner: ValueInt{
				Int: value.GetInt(),
			},
		}
	}

	return nil
}
