package postgres_test

import (
	"testing"

	postgres "github.com/Bofry/lib-postgres-stream"
)

func TestCreateReplicationSlotSourceProvider(t *testing.T) {
	var p postgres.CreateReplicationSlotSourceProvider
	p.AppendSource(postgres.CreateReplicationSlotSource{
		SlotName:       "foo",
		Plugin:         "wal2json",
		Temporary:      false,
		SlotType:       postgres.LogicalReplication,
		SnapshotAction: "",
	})

	t.Log(p.Sources())
}
