package client

import (
	"encoding/binary"
	"errors"
	"math/big"
	"strconv"

	"github.com/google/uuid"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"gopkg.in/inf.v0"
)

func ToUUID(val *pb.Value) (*uuid.UUID, error) {
	if val, ok := val.GetInner().(*pb.Value_Uuid); ok {
		mostSigBits := val.Uuid.Msb
		leastSigBits := val.Uuid.Lsb
		uuidStr := digits(mostSigBits>>int64(32), 8) + "-" + digits(mostSigBits>>int64(16), 4) + "-" + digits(mostSigBits, 4) + "-" + digits(leastSigBits>>int64(48), 4) + "-" + digits(leastSigBits, 12)

		parsedUUID := uuid.MustParse(uuidStr)
		return &parsedUUID, nil
	}

	return nil, errors.New("not a uuid")
}

func ToTimeUUID(val *pb.Value) (*uuid.UUID, error) {
	return ToUUID(val)
}

func digits(val uint64, digits int) string {
	high := uint64(1) << (digits * 4)
	str := strconv.FormatInt(int64(high|(val&(high-1))), 16)
	return str[1:]
}

func ToString(val *pb.Value) (string, error) {
	if val, ok := val.GetInner().(*pb.Value_String_); ok {
		return val.String_, nil
	}

	return "", errors.New("not a string")
}

func ToInt(val *pb.Value) (int64, error) {
	if val, ok := val.GetInner().(*pb.Value_Int); ok {
		return val.Int, nil
	}
	return 0, errors.New("not an int")
}

func ToBigInt(val *pb.Value) (*big.Int, error) {
	if val, ok := val.GetInner().(*pb.Value_Int); ok {
		return big.NewInt(val.Int), nil
	}
	return nil, errors.New("not a bigint")
}

func ToSmallInt(val *pb.Value) (int64, error) {
	if val, ok := val.GetInner().(*pb.Value_Int); ok {
		return val.Int, nil
	}
	return 0, errors.New("not a smallint")
}

func ToTinyInt(val *pb.Value) (int64, error) {
	if val, ok := val.GetInner().(*pb.Value_Int); ok {
		return val.Int, nil
	}
	return 0, errors.New("not a tinyint")
}

func ToBlob(val *pb.Value) ([]byte, error) {
	if val, ok := val.GetInner().(*pb.Value_Bytes); ok {
		return val.Bytes, nil
	}
	return nil, errors.New("not a blob")
}

func ToBoolean(val *pb.Value) (bool, error) {
	if val, ok := val.GetInner().(*pb.Value_Boolean); ok {
		return val.Boolean, nil
	}
	return false, errors.New("not a boolean")
}

func ToDecimal(val *pb.Value) (*inf.Dec, error) {
	if val, ok := val.GetInner().(*pb.Value_Decimal); ok {
		value, _ := binary.Uvarint(val.Decimal.Value)
		return inf.NewDec(int64(value), inf.Scale(val.Decimal.Scale)), nil
	}
	return nil, errors.New("not a decimal")
}

func ToDouble(val *pb.Value) (float64, error) {
	if val, ok := val.GetInner().(*pb.Value_Double); ok {
		return val.Double, nil
	}
	return 0, errors.New("not a double")
}

func ToFloat(val *pb.Value) (float32, error) {
	if val, ok := val.GetInner().(*pb.Value_Float); ok {
		return val.Float, nil
	}
	return 0, errors.New("not a float")
}

func ToInet(val *pb.Value) ([]byte, error) {
	if val, ok := val.GetInner().(*pb.Value_Bytes); ok {
		return val.Bytes, nil
	}
	return nil, errors.New("not an inet")
}

func ToVarInt(val *pb.Value) (uint64, error) {
	if val, ok := val.GetInner().(*pb.Value_Varint); ok {
		value, _ := binary.Uvarint(val.Varint.Value)
		return value, nil
	}
	return 0, errors.New("not a varint")
}

func ToDate(val *pb.Value) (uint32, error) {
	if val, ok := val.GetInner().(*pb.Value_Date); ok {
		return val.Date, nil
	}
	return 0, errors.New("not a date")
}

func ToTimestamp(val *pb.Value) (int64, error) {
	if val, ok := val.GetInner().(*pb.Value_Int); ok {
		return val.Int, nil
	}
	return 0, errors.New("not a timestamp")
}

func ToTime(val *pb.Value) (uint64, error) {
	if val, ok := val.GetInner().(*pb.Value_Time); ok {
		return val.Time, nil
	}
	return 0, errors.New("not a time")
}

func ToList(val *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	if _, ok := val.GetInner().(*pb.Value_Collection); ok {
		return translateType(val, spec)
	}
	return nil, errors.New("not a list")
}

func ToMap(val *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	if _, ok := val.GetInner().(*pb.Value_Collection); ok {
		return translateType(val, spec)
	}
	return nil, errors.New("not a map")
}

func ToSet(val *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	if _, ok := val.GetInner().(*pb.Value_Collection); ok {
		return translateType(val, spec)
	}
	return nil, errors.New("not a set")
}

func ToTuple(val *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	if _, ok := val.GetInner().(*pb.Value_Collection); ok {
		return translateType(val, spec)
	}
	return nil, errors.New("not a tuple")
}

func translateType(value *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	switch spec.GetSpec().(type) {
	case *pb.TypeSpec_Basic_:
		return translateBasicType(value, spec)
	case *pb.TypeSpec_Map_:
		elements := make(map[interface{}]interface{})

		for i := 0; i < len(value.GetCollection().Elements)-1; i += 2 {
			key, err := translateType(value.GetCollection().Elements[i], spec.GetMap().Key)
			if err != nil {
				return nil, err
			}
			mapVal, err := translateType(value.GetCollection().Elements[i+1], spec.GetMap().Value)
			if err != nil {
				return nil, err
			}
			elements[key] = mapVal
		}
		return elements, nil
	case *pb.TypeSpec_List_:
		var elements []interface{}

		for i := range value.GetCollection().Elements {
			element, err := translateType(value.GetCollection().Elements[i], spec.GetList().Element)
			if err != nil {
				return nil, err
			}
			elements = append(elements, element)
		}

		return elements, nil
	case *pb.TypeSpec_Set_:
		var elements []interface{}
		for _, element := range value.GetCollection().Elements {
			element, err := translateType(element, spec.GetSet().Element)
			if err != nil {
				return nil, err
			}

			elements = append(elements, element)
		}

		return elements, nil
	case *pb.TypeSpec_Udt_:
		fields := map[string]interface{}{}
		for key, val := range value.GetUdt().Fields {
			element, err := translateType(val, spec.GetUdt().Fields[key])
			if err != nil {
				return nil, err
			}

			fields[key] = element
		}

		return fields, nil
	case *pb.TypeSpec_Tuple_:
		var elements []interface{}
		numElements := len(spec.GetTuple().Elements)
		for i := 0; i <= len(value.GetCollection().Elements)-numElements; i++ {
			for j, typeSpec := range spec.GetTuple().Elements {
				element, err := translateType(value.GetCollection().Elements[i+j], typeSpec)
				if err != nil {
					return nil, err
				}

				elements = append(elements, element)
			}
		}

		return elements, nil
	}
	return nil, errors.New("unsupported type")
}

func translateBasicType(value *pb.Value, spec *pb.TypeSpec) (interface{}, error) {
	switch spec.GetBasic() {
	case pb.TypeSpec_CUSTOM:
		return ToBlob(value)
	case pb.TypeSpec_ASCII, pb.TypeSpec_TEXT, pb.TypeSpec_VARCHAR:
		return ToString(value)
	case pb.TypeSpec_BIGINT:
		return ToBigInt(value)
	case pb.TypeSpec_BLOB:
		return ToBlob(value)
	case pb.TypeSpec_BOOLEAN:
		return ToBoolean(value)
	case pb.TypeSpec_COUNTER:
		return ToInt(value)
	case pb.TypeSpec_DECIMAL:
		return ToDecimal(value)
	case pb.TypeSpec_DOUBLE:
		return ToDouble(value)
	case pb.TypeSpec_FLOAT:
		return ToFloat(value)
	case pb.TypeSpec_INT:
		return ToInt(value)
	case pb.TypeSpec_TIMESTAMP:
		return ToTimestamp(value)
	case pb.TypeSpec_UUID:
		return ToUUID(value)
	case pb.TypeSpec_VARINT:
		return ToVarInt(value)
	case pb.TypeSpec_TIMEUUID:
		return ToTimeUUID(value)
	case pb.TypeSpec_INET:
		return ToInet(value)
	case pb.TypeSpec_DATE:
		return ToDate(value)
	case pb.TypeSpec_TIME:
		return ToTime(value)
	case pb.TypeSpec_SMALLINT:
		return ToSmallInt(value)
	case pb.TypeSpec_TINYINT:
		return ToTinyInt(value)
	}

	return nil, errors.New("unsupported type")
}
