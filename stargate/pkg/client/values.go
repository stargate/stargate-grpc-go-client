package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/google/uuid"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"gopkg.in/inf.v0"
)

func ToUUID(val *pb.Value) (*uuid.UUID, error) {
	if val, ok := val.GetInner().(*pb.Value_Uuid); ok {
		parsedUUID, err := uuid.FromBytes(val.Uuid.GetValue())
		return &parsedUUID, err
	}

	return nil, errors.New("not a uuid")
}

func ToTimeUUID(val *pb.Value) (*uuid.UUID, error) {
	return ToUUID(val)
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
	if val, ok := val.GetInner().(*pb.Value_Inet); ok {
		return val.Inet.Value, nil
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

// StargateTypeSpec represents a type specification for a Stargate CQL data
// type.
type StargateTypeSpec interface {
	ToProto() *pb.TypeSpec
}

func scanTypeSpec(ts pb.TypeSpec_Basic) (StargateTypeSpec, error) {
	switch ts {
	case pb.TypeSpec_INT:
		return &IntType{}, nil
	case pb.TypeSpec_TEXT:
		return &TextType{}, nil
	}
	return nil, fmt.Errorf("unknown type spec: %v", ts)
}

// IntType is a StargateTypeSpec for an integer data type.
type IntType struct{}

// ToProto converts the IntType to a Stargate gRPC integer type spec.
func (t *IntType) ToProto() *pb.TypeSpec {
	return &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}}
}

// TextType is a StargateTypeSpec for a text data type.
type TextType struct{}

// ToProto converts the TextType to a Stargate gRPC text type spec.
func (t *TextType) ToProto() *pb.TypeSpec {
	return &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}}
}

// StargateColumnSpec represents the specification for a column in a Stargate
// table.
type StargateColumnSpec interface {
	ToProto() *pb.ColumnSpec
}

func scanColumnProto(c *pb.ColumnSpec) (StargateColumnSpec, error) {
	t := c.Type
	switch t.Spec.(type) {
	case *pb.TypeSpec_Basic_:
		ts, err := scanTypeSpec(t.GetBasic())
		if err != nil {
			return nil, fmt.Errorf("failed to scan basic type: %v", err)
		}
		return &BasicColumn{Name: c.Name, Type: ts}, nil
		// TODO: handle maps and lists
	}
	return nil, fmt.Errorf("unsupported column type: %v", t)
}

// BasicColumn is a StargateColumnSpec for a basic column with a standard type.
type BasicColumn struct {
	// Name is the name of the column.
	Name string
	// Type is the CQL data type of the column.
	Type StargateTypeSpec
}

// ToProto converts the BasicColumn to a Stargate gRPC column spec.
func (c *BasicColumn) ToProto() *pb.ColumnSpec {
	return &pb.ColumnSpec{
		Type: c.Type.ToProto(),
		Name: c.Name,
	}
}

// StargateValue represents a value in a Stargate table conforming to a standard
// CQL data type.
type StargateValue interface {
	ToProto() *pb.Value
}

// CassandraType is a type constraint for types that can translate to a CQL
// data type.
type CassandraType interface {
	string | // ASCII, TEXT, VARCHAR
		int64 // BIGINT, COUNTER, INT, SMALLINT, TINYINT, VARINT
}

// Value represents a basic value in a Stargate table.
type Value[T CassandraType] struct {
	value T
}

// NewValue creates a new basic value of the inferred type.
func NewValue[T CassandraType](value T) *Value[T] {
	return &Value[T]{value}
}

// ToProto converts the Value to a Stargate gRPC proto value.
func (v Value[T]) ToProto() *pb.Value {
	return valueToProto(v.value)
}

// TODO: add collection support

func valueToProto(value interface{}) *pb.Value {
	switch value.(type) {
	case string:
		return &pb.Value{Inner: &pb.Value_String_{String_: value.(string)}}
	case int64:
		return &pb.Value{Inner: &pb.Value_Int{Int: value.(int64)}}
	}
	return &pb.Value{Inner: &pb.Value_Null_{Null: &pb.Value_Null{}}}
}

func scanBasic(v *pb.Value) (StargateValue, error) {
	switch v.Inner.(type) {
	case *pb.Value_Int:
		return NewValue(v.GetInt()), nil
	case *pb.Value_String_:
		return NewValue(v.GetString_()), nil
		// TODO: handle other values, maps and lists
	}
	return nil, fmt.Errorf("unsupported value type: %T, value: %+v", v.Inner, v.Inner)
}

// Row represents a row in a Stargate table returned from a query in a
// StargateResponse.
type Row []StargateValue

func scanRowProto(r *pb.Row, colSpec []StargateColumnSpec) (Row, error) {
	vs := r.Values
	res := make([]StargateValue, len(vs))
	for i, v := range vs {
		var sv StargateValue
		var err error
		spec := colSpec[i]
		switch spec.(type) {
		case *BasicColumn:
			sv, err = scanBasic(v)
			// TODO: scan maps and lists
		}
		if err != nil {
			return nil, fmt.Errorf("failed to scan row value: %d:%v, error: %w", i, v, err)
		}
		res[i] = sv
	}
	return res, nil
}

// ToProto converts the Row to a Stargate gRPC row.
func (r *Row) ToProto() *pb.Row {
	row := &pb.Row{
		Values: make([]*pb.Value, len(*r)),
	}
	for i, cell := range *r {
		row.Values[i] = cell.ToProto()
	}
	return row
}

// StargateTableData represents a result from a Stargate query returned in a
// StargateResponse.
type StargateTableData struct {
	Columns []StargateColumnSpec
	Rows    []Row

	colIndex map[string]int
}

// ToProto converts the StargateTableData to a Stargate gRPC result.
func (d *StargateTableData) ToProto() *pb.ResultSet {
	res := &pb.ResultSet{
		Columns: make([]*pb.ColumnSpec, len(d.Columns)),
		Rows:    make([]*pb.Row, len(d.Rows)),
	}
	for i, col := range d.Columns {
		res.Columns[i] = col.ToProto()
	}
	for i, row := range d.Rows {
		res.Rows[i] = row.ToProto()
	}
	return res
}

// ValueReader creates a function that can read a .
func ValueReader[T CassandraType](
	result *StargateTableData,
	colName string,
) (func(row Row) T, error) {
	i, ok := result.colIndex[colName]
	if !ok {
		return nil, fmt.Errorf("column not found")
	}
	s := result.Columns[i]
	// TODO: validate type against type in column spec
	switch s.(type) {
	case *BasicColumn:
		return func(row Row) T {
			return row[i].(*Value[T]).value
		}, nil
	}
	return nil, fmt.Errorf("unknown column spec for reader: %+v", s)
}

// ScanResponseProto exctracts the result.
func ScanResponseProto(r *pb.Response) (*StargateTableData, error) {
	rs := r.GetResultSet()
	if rs == nil {
		return nil, fmt.Errorf("no result set in response")
	}

	cols := rs.Columns
	rows := rs.Rows
	res := &StargateTableData{
		Columns:  make([]StargateColumnSpec, len(cols)),
		Rows:     make([]Row, len(rows)),
		colIndex: make(map[string]int, len(cols)),
	}
	for i, col := range cols {
		c, err := scanColumnProto(col)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column: %d:%v, error: %w", i, col, err)
		}
		res.colIndex[col.Name] = i
		res.Columns[i] = c
	}
	for i, row := range rows {
		nr, err := scanRowProto(row, res.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %d:%v, error: %w", i, row, err)
		}
		res.Rows[i] = nr
	}

	return res, nil
}
