# lib-postgres-stream

`lib-postgres-stream` is a Go library for consuming PostgreSQL logical replication streams. It provides an easy-to-use API to connect to PostgreSQL, start a replication slot, and receive changes (WAL data) as a stream of messages.

## Features

- Easy subscription to PostgreSQL logical replication slots.
- Automatic Keepalive and Acknowledgement of Write-Ahead Logs (WAL).
- Concurrency-safe control mechanisms (`Pause()`, `Resume()`, `Close()`).
- Support for customizable replication options and starting LSN (Log Sequence Number) offsets.
- Custom message, event, and error handlers.

## Installation

```bash
go get github.com/Bofry/lib-postgres-stream
```

## Quick Start / Usage Example

Below is a simple example demonstrating how to configure and start consuming a PostgreSQL logical replication slot using `lib-postgres-stream`.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	postgres "github.com/Bofry/lib-postgres-stream"
)

func main() {
	// Configure the consumer
	consumer := &postgres.Consumer{
		MessageHandler: func(message *postgres.Message) {
			// Process incoming replication messages
			fmt.Println("Received data:", string(message.Body()))
            // The message has already been auto-acknowledged, but you can track LSNs.
            // LSN: message.StartLSN()
		},
		Logger: log.New(os.Stdout, "[postgres-stream] ", log.LstdFlags|log.Lmsgprefix),
		Config: &postgres.Config{
			Host:           "127.0.0.1",
			User:           "postgres",
			Password:       "postgres1234",
			Database:       "postgres",
			PollingTimeout: time.Second * 1,
			ReplicationOptions: postgres.ConfigureReplicationOptions().
				WithPluginArgs(`"pretty-print" 'true'`),
		},
	}

	// Subscribe to a specific replication slot
	err := consumer.Subscribe(
		postgres.SlotOffset{Slot: "my_replication_slot"},
	)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Listen for interrupt signals to gracefully shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	fmt.Println("Shutting down consumer...")
	consumer.Close()
	fmt.Println("Shutdown complete.")
}
```

## Configuration

The `postgres.Config` struct allows you to define connection details and polling behavior:

- `Host`: PostgreSQL server host (default: `127.0.0.1`).
- `Port`: PostgreSQL server port (default: `5432`).
- `Database`: Database name to connect to.
- `User`: PostgreSQL user.
- `Password`: PostgreSQL password.
- `ConnectTimeout`: Timeout for connecting to PostgreSQL.
- `PollingTimeout`: Timeout for reading messages from the stream (e.g., `time.Second * 1`).
- `ReplicationOptions`: Custom replication options, commonly generated using `postgres.ConfigureReplicationOptions()`.

## Slot Offset / Starting Position

When subscribing, you can dictate where the replication should begin by passing a `postgres.SlotOffset`.

Predefined offsets include:
- `StreamZeroOffset`: Starts from the beginning (0).
- `StreamNeverDeliveredOffset`: Starts from the current system XLogPos.
- `StreamUnspecifiedOffset`: Starts from the slot's confirmed flush LSN.

Example:
```go
postgres.SlotOffset{
    Slot: "my_replication_slot",
    LSN:  postgres.StreamUnspecifiedOffset,
}
```

## Control Flow

- **Pause()**: Temporarily pauses processing of the stream. Acknowledgements are still sent for the last flush LSN to keep the connection alive.
- **Resume()**: Resumes processing from where it was paused.
- **Close()**: Gracefully stops the background polling and closes the connection to the PostgreSQL database.
