package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	postgres "github.com/Bofry/lib-postgres-stream"
	"github.com/jackc/pglogrepl"
)

func TestConsumer(t *testing.T) {
	concumer := &postgres.Consumer{
		MessageHandler: func(message *postgres.Message) error {
			fmt.Println("data:", string(message.Bytes()))
			return nil
		},
		Logger: log.New(os.Stdout, "[test] ", log.Default().Flags()),
		Config: &postgres.Config{
			Host:     "192.168.56.58",
			User:     "postgres",
			Password: "postgres1234",
			Database: "postgres",
			// PollingTimeout: time.Millisecond * 10,
			PollingTimeout: time.Second * 3,
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 13*time.Second)

	err := concumer.Subscribe(
		postgres.SlotOffset{SlotName: "golang_replication_slot_temp"},
		postgres.WithPluginArgs(`"pretty-print" 'true'`),
		postgres.WithReplicationMode(pglogrepl.LogicalReplication),
	)
	if err != nil {
		t.Fatal(err)
	}

	<-ctx.Done()
	concumer.Close()
}
