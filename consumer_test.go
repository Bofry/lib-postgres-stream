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
	consumer := &postgres.Consumer{
		MessageHandler: func(message *postgres.Message) {
			fmt.Println("data:", string(message.Body()))
		},
		Logger: log.New(os.Stdout, "[test] ", log.Default().Flags()),
		Config: &postgres.Config{
			Host:           "192.168.56.58",
			User:           "postgres",
			Password:       "postgres1234",
			Database:       "postgres",
			PollingTimeout: time.Second * 1,
			ReplicationOptions: postgres.ConfigureReplicationOptions().
				WithPluginArgs(`"pretty-print" 'true'`),
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 13*time.Second)

	err := consumer.Subscribe(
		postgres.SlotOffset{Slot: "golang_replication_slot_temp"},
	)
	if err != nil {
		t.Fatal(err)
	}

	{
		consumer.Pause()
		time.AfterFunc(time.Second*8, func() {
			consumer.Resume()
		})
	}

	<-ctx.Done()
	consumer.Close()
}
