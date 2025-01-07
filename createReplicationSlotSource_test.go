package postgres

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
)

func TestCreateReplicationSlotSource(t *testing.T) {
	var data = `{
	"SlotName": "foo",
	"Plugin": "wal2json",
	"Temporary": false,
	"SlotType": "logical"
}
`

	var source CreateReplicationSlotSource
	err := json.Unmarshal([]byte(data), &source)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- source:\n%+v\n\n", source)

}

func TestCreateReplicationSlotSource_Set(t *testing.T) {
	var data = `[
{
	"SlotName": "foo",
	"Plugin": "wal2json",
	"Temporary": false,
	"SlotType": "logical"
},
{
	"SlotName": "bar",
	"Plugin": "wal2json",
	"Temporary": false,
	"SlotType": "logical"
}
]
`

	var source []CreateReplicationSlotSource
	err := json.Unmarshal([]byte(data), &source)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- source:\n%+v\n\n", source)

}
