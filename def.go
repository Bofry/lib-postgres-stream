package postgres

import (
	"log"
	"os"

	"github.com/jackc/pglogrepl"
)

const (
	LOGGER_PREFIX string = "[lib-postgres-stream] "

	__SQL_REPLICATION_SLOT_CONFIRMED_FLUSH_LSN string = `SELECT confirmed_flush_lsn FROM "pg_catalog"."pg_replication_slots" WHERE slot_name ='%s';`

	StreamZeroOffset           string = "0"
	StreamNeverDeliveredOffset string = ">"
	StreamUnspecifiedOffset    string = ""
)

var (
	defaultLogger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	LSN = pglogrepl.LSN

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
