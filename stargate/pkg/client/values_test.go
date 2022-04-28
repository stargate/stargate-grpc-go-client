package client

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func ExampleValueReader() {
	// In a real application:
	// res, err := c.ExecuteQuery(...)
	// if err != nil {...}
	// d := ScanResponseProto(res)
	d := &StargateTableData{
		Columns: []StargateColumnSpec{
			&BasicColumn{Name: "text_col", Type: &TextType{}},
			&BasicColumn{Name: "int_col", Type: &IntType{}},
		},
		Rows: []Row{
			{NewValue("aaa"), NewValue(int64(111))},
			{NewValue("bbb"), NewValue(int64(222))},
		},
		colIndex: map[string]int{"text_col": 0, "int_col": 1},
	}

	textReader, err := ValueReader[string](d, "text_col")
	if err != nil {
		fmt.Printf("ValueReader[string] error: %v", err)
		return
	}
	intReader, err := ValueReader[int64](d, "int_col")
	if err != nil {
		fmt.Printf("ValueReader[int64] error: %v", err)
		return
	}

	for _, r := range d.Rows {
		t := textReader(r)
		i := intReader(r)
		fmt.Printf("%v, %v\n", t, i)
	}

	// Output:
	// aaa, 111
	// bbb, 222
}

func TestScanResponseProto(t *testing.T) {
	res := &pb.Response{
		Result: &pb.Response_ResultSet{
			ResultSet: &pb.ResultSet{
				Columns: []*pb.ColumnSpec{
					{
						Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}},
						Name: "text_col",
					},
					{
						Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}},
						Name: "int_col",
					},
				},
				Rows: []*pb.Row{
					{
						Values: []*pb.Value{
							{Inner: &pb.Value_String_{String_: "aaa"}}, // text_col
							{Inner: &pb.Value_Int{Int: 111}},           // int_col
						},
					},
					{
						Values: []*pb.Value{
							{Inner: &pb.Value_String_{String_: "bbb"}}, // text_col
							{Inner: &pb.Value_Int{Int: 222}},           // int_col
						},
					},
				},
			},
		},
	}

	got, err := ScanResponseProto(res)
	if err != nil {
		fmt.Printf("ScanResponseProto() error: %v", err)
		return
	}

	want := &StargateTableData{
		Columns: []StargateColumnSpec{
			&BasicColumn{Name: "text_col", Type: &TextType{}},
			&BasicColumn{Name: "int_col", Type: &IntType{}},
		},
		Rows: []Row{
			{NewValue("aaa"), NewValue(int64(111))},
			{NewValue("bbb"), NewValue(int64(222))},
		},
		colIndex: map[string]int{"text_col": 0, "int_col": 1},
	}

	opt := cmp.AllowUnexported(StargateTableData{}, Value[string]{}, Value[int64]{})
	if diff := cmp.Diff(want, got, opt); diff != "" {
		t.Fatalf("ScanResponseProto() unexpected difference (-want +got):\n%v", diff)
	}
}

func TestStargateTableData_ToProto(t *testing.T) {
	d := &StargateTableData{
		Columns: []StargateColumnSpec{
			&BasicColumn{Name: "text_col", Type: &TextType{}},
			&BasicColumn{Name: "int_col", Type: &IntType{}},
		},
		Rows: []Row{
			{NewValue("aaa"), NewValue(int64(111))},
			{NewValue("bbb"), NewValue(int64(222))},
		},
	}

	got := d.ToProto()

	want := &pb.ResultSet{
		Columns: []*pb.ColumnSpec{
			{
				Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}},
				Name: "text_col",
			},
			{
				Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}},
				Name: "int_col",
			},
		},
		Rows: []*pb.Row{
			{
				Values: []*pb.Value{
					{Inner: &pb.Value_String_{String_: "aaa"}},
					{Inner: &pb.Value_Int{Int: 111}},
				},
			},
			{
				Values: []*pb.Value{
					{Inner: &pb.Value_String_{String_: "bbb"}},
					{Inner: &pb.Value_Int{Int: 222}},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf(
			"ToProto() unexpected difference (-want +got):\n%v",
			diff,
		)
	}
}
