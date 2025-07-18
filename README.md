# lib-postgres-stream

`lib-postgres-stream` is a Go library for consuming PostgreSQL logical replication streams. It wraps `pglogrepl` and handles polling, message acknowledgment and slot management so applications can easily consume database change events.

## Installation

```bash
go get github.com/Bofry/lib-postgres-stream
```

## Configuration

Create a `Consumer` with a `Config` describing connection information and replication options.

```go
consumer := &postgres.Consumer{
    MessageHandler: func(m *postgres.Message) {
        fmt.Println("data:", string(m.Body()))
    },
    Config: &postgres.Config{
        Host:     "127.0.0.1",
        User:     "postgres",
        Password: "secret",
        Database: "postgres",
        PollingTimeout: time.Second,
        ReplicationOptions: postgres.ConfigureReplicationOptions().
            WithPluginArgs(`"pretty-print" 'true'`),
    },
}
```

`ReplicationOption`s are applied to `pglogrepl.StartReplicationOptions`. Built-in helpers include:

- `WithPluginArgs(args ...string)` – pass arguments to the logical decoding plugin.
- `WithReplicationMode(mode pglogrepl.ReplicationMode)` – set replication mode.

## Usage

```go
err := consumer.Subscribe(postgres.SlotOffset{Slot: "example_slot"})
if err != nil {
    log.Fatal(err)
}

consumer.Pause()
time.AfterFunc(5*time.Second, func() { consumer.Resume() })

// run until done
<-time.After(10 * time.Second)
consumer.Close()
```

## WAL2JSON helpers

The `waldata` subpackage contains utilities for parsing `wal2json` output and binding the result to structs.

```go
import "github.com/Bofry/lib-postgres-stream/waldata"

set, err := waldata.ExtractWal2JsonData(buf)
```

## License

This project is licensed under the MIT License. Contributions are welcome via pull requests.
