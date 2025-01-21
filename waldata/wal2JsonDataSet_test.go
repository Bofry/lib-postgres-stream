package waldata_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Bofry/lib-postgres-stream/waldata"
)

func TestWal2JsonDataSet_Empty(t *testing.T) {
	var input = []byte(`
	{
		"change":[]
	}`)
	var data waldata.Wal2JsonDataSet

	err := json.Unmarshal(input, &data)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := waldata.Wal2JsonDataSet{}
	if !reflect.DeepEqual(expected, data) {
		t.Errorf("expected: %+v, got: %+v", expected, data)
	}
}

func TestWal2JsonDataSet_Insert(t *testing.T) {
	var input = []byte(`
	{
		"change":[
			{
				"kind":"insert",
				"schema":"public",
				"table":"t",
				"columnnames":["id","name"],
				"columntypes":["integer","text"],
				"columnvalues":[1,"foo"]
			}
		]
	}`)
	var data waldata.Wal2JsonDataSet

	err := json.Unmarshal(input, &data)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := waldata.Wal2JsonDataSet{
		waldata.Wal2JsonData{
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
		},
	}
	if !reflect.DeepEqual(expected, data) {
		t.Errorf("expected: %+v, got: %+v", expected, data)
	}
}

func TestWal2JsonDataSet_Update(t *testing.T) {
	var input = []byte(`
	{
		"change":[
			{
				"kind":"update",
				"schema":"public",
				"table":"t",
				"columnnames":["id","name"],
				"columntypes":["integer","text"],
				"columnvalues":[1,"bar"],
				"oldkeys":{
					"keynames":["id"],
					"keytypes":["integer"],
					"keyvalues":[1]
				}
			}
		]
	}`)
	var data waldata.Wal2JsonDataSet

	err := json.Unmarshal(input, &data)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := waldata.Wal2JsonDataSet{
		waldata.Wal2JsonData{
			Kind:   "update",
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
					Value: "bar",
				},
			},
			Keys: map[string]waldata.Wal2JsonDataField{
				"id": {
					Name:  "id",
					Type:  "integer",
					Value: json.Number("1"),
				},
			},
		},
	}
	if !reflect.DeepEqual(expected, data) {
		t.Errorf("expected: %+v, got: %+v", expected, data)
	}
}

func TestWal2JsonDataSet_Delete(t *testing.T) {
	var input = []byte(`
	{
		"change":[
			{
				"kind":"delete",
				"schema":"public",
				"table":"t",
				"oldkeys":{
					"keynames":["id"],
					"keytypes":["integer"],
					"keyvalues":[1]
				}
			}
		]
	}`)
	var data waldata.Wal2JsonDataSet

	err := json.Unmarshal(input, &data)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := waldata.Wal2JsonDataSet{
		waldata.Wal2JsonData{
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
		},
	}
	if !reflect.DeepEqual(expected, data) {
		t.Errorf("expected: %+v, got: %+v", expected, data)
	}
}
