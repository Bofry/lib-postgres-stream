package postgres

import (
	"log"
	"os"

	"github.com/jackc/pglogrepl"
)

const (
	LOGGER_PREFIX string = "[lib-postgres-stream] "

	__SQL_SELECT_REPLICATION_SLOT string = `
SELECT slot_name,
       plugin,
			 slot_type,
			 database,
			 temporary,
			 active,
			 restart_lsn,
			 confirmed_flush_lsn
  FROM "pg_catalog"."pg_replication_slots"
WHERE slot_name IN (%s);`
	__SQL_CHECK_MISSING_REPLICATION_SLOT string = `
SELECT list.slot_name
  FROM (
    SELECT unnest(ARRAY[%s])::name AS slot_name
  ) AS list
  LEFT JOIN "pg_catalog"."pg_replication_slots" AS slot
    ON list.slot_name = slot.slot_name
 WHERE slot.slot_name IS NULL;`

	__PG_ERRCODE_DUPLICATE_OBJECT = "42710"

	StreamZeroOffset           string = "0"
	StreamNeverDeliveredOffset string = ">"
	StreamUnspecifiedOffset    string = ""
)

const (
	LogicalReplication  = pglogrepl.LogicalReplication
	PhysicalReplication = pglogrepl.PhysicalReplication

	Wal2JsonPlugin = "wal2json"
)

var (
	defaultLogger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	LSN             = pglogrepl.LSN
	ReplicationMode = pglogrepl.ReplicationMode

	MessageHandleProc func(message *Message) error
	EventHandleProc   func(event Event) error
	ErrorHandleProc   func(err error) (disposed bool)

	MessageDelegate interface {
		OnAck(msg *Message)
	}

	Event interface {
		ByteID() byte
	}

	SlotOffsetInfo interface {
		getSlotOffset() SlotOffset
	}

	ReplicationOption interface {
		applyStartReplicationOptions(opt *pglogrepl.StartReplicationOptions)
	}
)
