package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	postgres "github.com/Bofry/lib-postgres-stream"
)

func TestConsumer(t *testing.T) {
	concumer := &postgres.Consumer{
		DisableAutoAck: false,
		MessageHandler: func(message *postgres.Message) error {
			fmt.Println("data:", string(message.Body()))
			return nil
		},
		Logger: log.New(os.Stdout, "[test] ", log.Default().Flags()),
		Config: &postgres.Config{
			Host:           "192.168.56.58",
			User:           "postgres",
			Password:       "postgres1234",
			Database:       "postgres",
			PollingTimeout: time.Second * 1,
			ReplicationOptions: postgres.ConfigureReplicationOptions().
				WithPluginArgs(`"pretty-print" 'true'`).
				WithReplicationMode(postgres.LogicalReplication),
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 13*time.Second)

	err := concumer.Subscribe(
		postgres.SlotOffset{Slot: "golang_replication_slot_temp"},
	)
	if err != nil {
		t.Fatal(err)
	}

	<-ctx.Done()
	concumer.Close()
}
