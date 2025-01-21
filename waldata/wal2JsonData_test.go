package waldata_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Bofry/lib-postgres-stream/waldata"
)

func TestWal2JsonData_FillFields(t *testing.T) {
	data := waldata.Wal2JsonData{
		Kind:   "insert",
		Schema: "public",
		Table:  "t",
		Fields: map[string]waldata.Wal2JsonDataField{
			"id": {
				Name:  "id",
				Type:  "integer",
				Value: json.Number("1"),
			},
			"name": {
				Name:  "name",
				Type:  "text",
				Value: "foo",
			},
		},
	}

	type Record struct {
		ID   int32  `key:"id"`
		Name string `key:"name"`
	}

	var v Record
	err := data.FillFields(&v)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Record{
		ID:   1,
		Name: "foo",
	}
	if !reflect.DeepEqual(expected, v) {
		t.Errorf("expected: %+v, got: %+v", expected, v)
	}
}

func TestWal2JsonData_FillFields_WithNil(t *testing.T) {
	data := waldata.Wal2JsonData{
		Kind:   "insert",
		Schema: "public",
		Table:  "t",
	}

	type Record struct {
		ID   int32  `key:"id"`
		Name string `key:"name"`
	}

	var v Record
	err := data.FillFields(&v)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Record{}
	if !reflect.DeepEqual(expected, v) {
		t.Errorf("expected: %+v, got: %+v", expected, v)
	}
}

func TestWal2JsonData_FillKeys(t *testing.T) {
	data := waldata.Wal2JsonData{
		Kind:   "delete",
		Schema: "public",
		Table:  "t",
		Keys: map[string]waldata.Wal2JsonDataField{
			"id": {
				Name:  "id",
				Type:  "integer",
				Value: json.Number("1"),
			},
		},
	}

	type Record struct {
		ID int32 `key:"id"`
	}

	var v Record
	err := data.FillKeys(&v)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Record{
		ID: 1,
	}
	if !reflect.DeepEqual(expected, v) {
		t.Errorf("expected: %+v, got: %+v", expected, v)
	}
}

func TestWal2JsonData_FillKeys_WithNil(t *testing.T) {
	data := waldata.Wal2JsonData{
		Kind:   "delete",
		Schema: "public",
		Table:  "t",
	}

	type Record struct {
		ID int32 `key:"id"`
	}

	var v Record
	err := data.FillKeys(&v)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := Record{}
	if !reflect.DeepEqual(expected, v) {
		t.Errorf("expected: %+v, got: %+v", expected, v)
	}
}
