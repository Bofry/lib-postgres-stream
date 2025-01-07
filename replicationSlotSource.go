package postgres

import (
	"github.com/jackc/pglogrepl"
)

type ReplicationSlotSource struct {
	SlotName          string
	Plugin            string
	SlotType          pglogrepl.ReplicationMode
	Database          string
	Temporary         bool
	Active            bool
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
	startLSN          pglogrepl.LSN
}
