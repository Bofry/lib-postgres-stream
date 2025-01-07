package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestSelectReplicationSlot(t *testing.T) {
	var (
		Host            = "192.168.56.58"
		Port     uint16 = 5432
		User            = "postgres"
		Password        = "postgres1234"
		Database        = "postgres"
	)
	config, err := pgconn.ParseConfig(fmt.Sprintf("postgres://%s?replication=database", Host))
	if err != nil {
		panic(err)
	}
	config.Port = Port
	config.User = User
	config.Password = Password
	config.Database = Database

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}

	records, err := SelectReplicationSlot(context.Background(), conn, []string{"a", "b", "golang_replication_slot_temp"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v\n", records)
}

func TestCheckMissingReplicationSlot(t *testing.T) {
	var (
		Host            = "192.168.56.58"
		Port     uint16 = 5432
		User            = "postgres"
		Password        = "postgres1234"
		Database        = "postgres"
	)
	config, err := pgconn.ParseConfig(fmt.Sprintf("postgres://%s?replication=database", Host))
	if err != nil {
		panic(err)
	}
	config.Port = Port
	config.User = User
	config.Password = Password
	config.Database = Database

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}

	missingSlots, err := CheckMissingReplicationSlot(context.Background(), conn, []string{"a", "b", "golang_replication_slot_temp"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v\n", missingSlots)
}
