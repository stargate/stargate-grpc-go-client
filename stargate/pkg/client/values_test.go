package client

import (
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

func Test_translateType(t *testing.T) {
	type args struct {
		columnSpec *pb.ColumnSpec
		value      *pb.Value
	}
	tests := []struct {
		name string
		args args
		want *Value
	}{
		{
			name: "Custom",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_CUSTOM,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Bytes{
						Bytes: []byte{0, 1},
					},
				},
			},
			want: &Value{
				Inner: ValueBytes{Bytes: []byte{0, 1}},
			},
		},
		{
			name: "ASCII",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_ASCII,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_String_{
						String_: "foo",
					},
				},
			},
			want: &Value{
				Inner: ValueString{
					String: "foo",
				},
			},
		},
		{
			name: "BIGINT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_BIGINT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: 2,
				},
			},
		},
		{
			name: "BLOB",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_BLOB,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Bytes{
						Bytes: []byte{0, 1},
					},
				},
			},
			want: &Value{
				Inner: ValueBytes{Bytes: []byte{0, 1}},
			},
		},
		{
			name: "BOOLEAN",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_BOOLEAN,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Boolean{
						Boolean: true,
					},
				},
			},
			want: &Value{
				Inner: ValueBoolean{
					Boolean: true,
				},
			},
		},
		{
			name: "COUNTER",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_COUNTER,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: 2,
				},
			},
		},
		{
			name: "DOUBLE",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_DOUBLE,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Double{
						Double: float64(3.14),
					},
				},
			},
			want: &Value{
				Inner: ValueDouble{
					Double: float64(3.14),
				},
			},
		},
		{
			name: "FLOAT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_FLOAT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Float{
						Float: float32(3.14),
					},
				},
			},
			want: &Value{
				Inner: ValueFloat{
					Float: float32(3.14),
				},
			},
		},
		{
			name: "INT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_INT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: int64(2),
				},
			},
		},
		{
			name: "TEXT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_TEXT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_String_{
						String_: "foo",
					},
				},
			},
			want: &Value{
				Inner: ValueString{
					String: "foo",
				},
			},
		},
		{
			name: "TIMESTAMP",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_TIMESTAMP,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: 2,
				},
			},
		},
		{
			name: "UUID",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_UUID,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Uuid{
						Uuid: &pb.Uuid{
							Msb: uint64(64),
							Lsb: uint64(32),
						},
					},
				},
			},
			want: &Value{
				Inner: ValueUUID{
					UUID: &Uuid{
						Msb: uint64(64),
						Lsb: uint64(32),
					},
				},
			},
		},
		{
			name: "VARCHAR",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_VARCHAR,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_String_{
						String_: "foo",
					},
				},
			},
			want: &Value{
				Inner: ValueString{
					String: "foo",
				},
			},
		},
		{
			name: "TIMEUUID",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_TIMEUUID,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Uuid{
						Uuid: &pb.Uuid{
							Msb: uint64(64),
							Lsb: uint64(32),
						},
					},
				},
			},
			want: &Value{
				Inner: ValueUUID{
					UUID: &Uuid{
						Msb: uint64(64),
						Lsb: uint64(32),
					},
				},
			},
		},
		{
			name: "INET",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_INET,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Bytes{
						Bytes: []byte{0, 1},
					},
				},
			},
			want: &Value{
				Inner: ValueBytes{
					Bytes: []byte{0, 1},
				},
			},
		},
		{
			name: "DATE",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_DATE,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Date{
						Date: uint32(200),
					},
				},
			},
			want: &Value{
				Inner: ValueDate{
					Date: uint32(200),
				},
			},
		},
		{
			name: "TIME",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_TIME,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Time{
						Time: uint64(500),
					},
				},
			},
			want: &Value{
				Inner: ValueTime{
					Time: uint64(500),
				},
			},
		},
		{
			name: "SMALLINT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_SMALLINT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: 2,
				},
			},
		},
		{
			name: "TINYINT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{
							Basic: pb.TypeSpec_TINYINT,
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Int{
						Int: int64(2),
					},
				},
			},
			want: &Value{
				Inner: ValueInt{
					Int: 2,
				},
			},
		},
		{
			name: "Collections - Map",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Map_{
							Map: &pb.TypeSpec_Map{
								Key: &pb.TypeSpec{
									Spec: &pb.TypeSpec_Basic_{
										Basic: pb.TypeSpec_TEXT,
									},
								},
								Value: &pb.TypeSpec{
									Spec: &pb.TypeSpec_Basic_{
										Basic: pb.TypeSpec_DOUBLE,
									},
								},
							},
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Collection{
						Collection: &pb.Collection{
							Elements: []*pb.Value{
								{
									Inner: &pb.Value_String_{
										String_: "foo",
									},
								},
								{
									Inner: &pb.Value_Double{
										Double: 3.14,
									},
								},
							},
						},
					},
				},
			},
			want: &Value{
				Inner: ValueCollection{
					Collection: &Collection{
						Elements: []*Value {
							{
								Inner: ValueString{String: "foo"},
							},
							{
								Inner: ValueDouble{Double: 3.14},
							},
						},
					},
				},
			},
		},
		{
			name: "Collections - List",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_List_{
							List: &pb.TypeSpec_List{
								Element: &pb.TypeSpec{
									Spec: &pb.TypeSpec_Basic_{
										Basic: pb.TypeSpec_DOUBLE,
									},
								},
							},
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Collection{
						Collection: &pb.Collection{
							Elements: []*pb.Value{
								{
									Inner: &pb.Value_Double{
										Double: 1.2,
									},
								},
								{
									Inner: &pb.Value_Double{
										Double: 2.1,
									},
								},
							},
						},
					},
				},
			},
			want: &Value{
				Inner: ValueCollection{
					Collection: &Collection{
						Elements: []*Value {
							{
								Inner: ValueDouble{Double: 1.2},
							},
							{
								Inner: ValueDouble{Double: 2.1},
							},
						},
					},
				},
			},
		},
		{
			name: "Collections - Set",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Set_{
							Set: &pb.TypeSpec_Set{
								Element: &pb.TypeSpec{
									Spec: &pb.TypeSpec_Basic_{
										Basic: pb.TypeSpec_INET,
									},
								},
							},
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Collection{
						Collection: &pb.Collection{
							Elements: []*pb.Value{
								{
									Inner: &pb.Value_Bytes{
										Bytes: []byte{0,1,2},
									},
								},
								{
									Inner: &pb.Value_Bytes{
										Bytes: []byte{3,4,5},
									},
								},
							},
						},
					},
				},
			},
			want: &Value{
				Inner: ValueCollection{
					Collection: &Collection{
						Elements: []*Value {
							{
								Inner: ValueBytes{Bytes: []byte{0,1,2}},
							},
							{
								Inner: ValueBytes{Bytes: []byte{3,4,5}},
							},
						},
					},
				},
			},
		},
		{
			name: "Collections - Tuple",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Tuple_{
							Tuple: &pb.TypeSpec_Tuple{
								Elements: []*pb.TypeSpec{
									{
										Spec: &pb.TypeSpec_Basic_{
											Basic: pb.TypeSpec_VARCHAR,
										},
									},
									{
										Spec: &pb.TypeSpec_Basic_{
											Basic: pb.TypeSpec_INET,
										},
									},
									{
										Spec: &pb.TypeSpec_Basic_{
											Basic: pb.TypeSpec_INT,
										},
									},
								},
							},
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Collection{
						Collection: &pb.Collection{
							Elements: []*pb.Value{
								{
									Inner: &pb.Value_String_{
										String_: "bar",
									},
								},
								{
									Inner: &pb.Value_Bytes{
										Bytes: []byte{0,1},
									},
								},
								{
									Inner: &pb.Value_Int{
										Int: 3,
									},
								},
							},
						},
					},
				},
			},
			want: &Value{
				Inner: ValueCollection{
					Collection: &Collection{
						Elements: []*Value {
							{
								Inner: ValueString{String: "bar"},
							},
							{
								Inner: ValueBytes{Bytes: []byte{0,1}},
							},
							{
								Inner: ValueInt{Int: 3},
							},
						},
					},
				},
			},
		}, {
			name: "UDT",
			args: args{
				columnSpec: &pb.ColumnSpec{
					Type: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Udt_{
							Udt: &pb.TypeSpec_Udt{
								Fields: map[string]*pb.TypeSpec{
									"field1": {
										Spec: &pb.TypeSpec_Basic_{
											Basic: pb.TypeSpec_INT,
										},
									},
									"field2": {
										Spec: &pb.TypeSpec_Basic_{
											Basic: pb.TypeSpec_VARCHAR,
										},
									},
								},
							},
						},
					},
				},
				value: &pb.Value{
					Inner: &pb.Value_Udt{
						Udt: &pb.UdtValue{
							Fields: map[string]*pb.Value{
								"field1": {
									Inner: &pb.Value_Int{
										Int: 3,
									},
								},
								"field2": {
									Inner: &pb.Value_String_{
										String_: "foo",
									},
								},
							},
						},
					},
				},
			},
			want: &Value{
				Inner: ValueUdt{
					UDT: &UdtValue{
						Fields: map[string]*Value{
							"field1": {
								Inner: ValueInt{
									Int: 3,
								},
							},
							"field2": {
								Inner: ValueString{
									String: "foo",
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := translateType(tt.args.columnSpec.Type, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				log.WithField("want", tt.want).WithField("got", got).Infof("foo")
				t.Errorf("translateType() = %v, want %v", got, tt.want)
			}
		})
	}
}
