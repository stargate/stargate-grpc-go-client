package client

import (
	"reflect"
	"testing"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

func Test_mapTypeSpec(t *testing.T) {
	type args struct {
		spec *pb.TypeSpec
	}
	tests := []struct {
		name string
		args args
		want TypeSpec
	}{
		{
			name: "Custom",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_CUSTOM,
					},
				},
			},
			want: TypeSpecBasic{CUSTOM},
		},
		{
			name: "ASCII",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_ASCII,
					},
				},
			},
			want: TypeSpecBasic{ASCII},
		},
		{
			name: "BIGINT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_BIGINT,
					},
				},
			},
			want: TypeSpecBasic{BIGINT},
		},
		{
			name: "BLOB",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_BLOB,
					},
				},
			},
			want: TypeSpecBasic{BLOB},
		},
		{
			name: "BOOLEAN",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_BOOLEAN,
					},
				},
			},
			want: TypeSpecBasic{BOOLEAN},
		},
		{
			name: "COUNTER",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_COUNTER,
					},
				},
			},
			want: TypeSpecBasic{COUNTER},
		},
		{
			name: "DECIMAL",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_DECIMAL,
					},
				},
			},
			want: TypeSpecBasic{DECIMAL},
		},
		{
			name: "DOUBLE",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_DOUBLE,
					},
				},
			},
			want: TypeSpecBasic{DOUBLE},
		},
		{
			name: "FLOAT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_FLOAT,
					},
				},
			},
			want: TypeSpecBasic{FLOAT},
		},
		{
			name: "INT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_INT,
					},
				},
			},
			want: TypeSpecBasic{INT},
		},
		{
			name: "TEXT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_TEXT,
					},
				},
			},
			want: TypeSpecBasic{TEXT},
		},
		{
			name: "TIMESTAMP",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_TIMESTAMP,
					},
				},
			},
			want: TypeSpecBasic{TIMESTAMP},
		},
		{
			name: "UUID",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_UUID,
					},
				},
			},
			want: TypeSpecBasic{UUID},
		},
		{
			name: "VARCHAR",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_VARCHAR,
					},
				},
			},
			want: TypeSpecBasic{VARCHAR},
		},
		{
			name: "VARINT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_VARINT,
					},
				},
			},
			want: TypeSpecBasic{VARINT},
		},
		{
			name: "TIMEUUID",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_TIMEUUID,
					},
				},
			},
			want: TypeSpecBasic{TIMEUUID},
		},
		{
			name: "INET",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_INET,
					},
				},
			},
			want: TypeSpecBasic{INET},
		},
		{
			name: "DATE",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_DATE,
					},
				},
			},
			want: TypeSpecBasic{DATE},
		},
		{
			name: "TIME",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_TIME,
					},
				},
			},
			want: TypeSpecBasic{TIME},
		},
		{
			name: "SMALLINT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_SMALLINT,
					},
				},
			},
			want: TypeSpecBasic{SMALLINT},
		},
		{
			name: "TINYINT",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{
						Basic: pb.TypeSpec_TINYINT,
					},
				},
			},
			want: TypeSpecBasic{TINYINT},
		},
		{
			name: "Collections - Map",
			args: args{
				spec: &pb.TypeSpec{
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
			want: TypeSpecMap{
				Key:   TypeSpecBasic{TEXT},
				Value: TypeSpecBasic{DOUBLE},
			},
		},
		{
			name: "Collections - List",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_List_{
						List: &pb.TypeSpec_List{
							Element: &pb.TypeSpec{
								Spec: &pb.TypeSpec_Basic_{
									Basic: pb.TypeSpec_VARCHAR,
								},
							},
						},
					},
				},
			},
			want: TypeSpecList{
				Element: TypeSpecBasic{VARCHAR},
			},
		},
		{
			name: "Collections - List of lists",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_List_{
						List: &pb.TypeSpec_List{
							Element: &pb.TypeSpec{
								Spec: &pb.TypeSpec_List_{
									List: &pb.TypeSpec_List{
										Element: &pb.TypeSpec{
											Spec: &pb.TypeSpec_Basic_{
												Basic: pb.TypeSpec_VARCHAR,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: TypeSpecList{
				Element: TypeSpecList{Element: TypeSpecBasic{VARCHAR}},
			},
		},
		{
			name: "Collections - Set",
			args: args{
				spec: &pb.TypeSpec{
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
			want: TypeSpecSet{
				Element: TypeSpecBasic{INET},
			},
		},
		{
			name: "Collections - Tuple",
			args: args{
				spec: &pb.TypeSpec{
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
			want: TypeSpecTuple{
				Elements: []TypeSpec{
					TypeSpecBasic{VARCHAR},
					TypeSpecBasic{INET},
					TypeSpecBasic{INT},
				},
			},
		},
		{
			name: "UDT",
			args: args{
				spec: &pb.TypeSpec{
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
			want: TypeSpecUdt{
				Fields: map[string]TypeSpec{
					"field1": TypeSpecBasic{INT},
					"field2": TypeSpecBasic{VARCHAR},
				},
			},
		},
		{
			name: "UDT with collection",
			args: args{
				spec: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Udt_{
						Udt: &pb.TypeSpec_Udt{
							Fields: map[string]*pb.TypeSpec{
								"field1": {
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
			want: TypeSpecUdt{
				Fields: map[string]TypeSpec{
					"field1": TypeSpecSet{Element: TypeSpecBasic{INET}},
					"field2": TypeSpecBasic{VARCHAR},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapTypeSpec(tt.args.spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapTypeSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}
