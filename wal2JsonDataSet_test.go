package postgres_test

import (
	"encoding/json"
	"reflect"
	"testing"

	postgres "github.com/Bofry/lib-postgres-stream"
)

func TestWal2JsonDataSet_Empty(t *testing.T) {
	var input = []byte(`
	{
		"change":[]
	}`)
	var content postgres.Wal2JsonDataSet

	err := json.Unmarshal(input, &content)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := postgres.Wal2JsonDataSet{}
	if !reflect.DeepEqual(expected, content) {
		t.Errorf("expected: %+v, got: %+v", expected, content)
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
	var content postgres.Wal2JsonDataSet

	err := json.Unmarshal(input, &content)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := postgres.Wal2JsonDataSet{
		postgres.Wal2JsonData{
			Kind:   "insert",
			Schema: "public",
			Table:  "t",
			Fields: map[string]postgres.Wal2JsonDataField{
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
	if !reflect.DeepEqual(expected, content) {
		t.Errorf("expected: %+v, got: %+v", expected, content)
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
	var content postgres.Wal2JsonDataSet

	err := json.Unmarshal(input, &content)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := postgres.Wal2JsonDataSet{
		postgres.Wal2JsonData{
			Kind:   "update",
			Schema: "public",
			Table:  "t",
			Fields: map[string]postgres.Wal2JsonDataField{
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
			Keys: map[string]postgres.Wal2JsonDataField{
				"id": {
					Name:  "id",
					Type:  "integer",
					Value: json.Number("1"),
				},
			},
		},
	}
	if !reflect.DeepEqual(expected, content) {
		t.Errorf("expected: %+v, got: %+v", expected, content)
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
	var content postgres.Wal2JsonDataSet

	err := json.Unmarshal(input, &content)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	expected := postgres.Wal2JsonDataSet{
		postgres.Wal2JsonData{
			Kind:   "delete",
			Schema: "public",
			Table:  "t",
			Keys: map[string]postgres.Wal2JsonDataField{
				"id": {
					Name:  "id",
					Type:  "integer",
					Value: json.Number("1"),
				},
			},
		},
	}
	if !reflect.DeepEqual(expected, content) {
		t.Errorf("expected: %+v, got: %+v", expected, content)
	}
}
