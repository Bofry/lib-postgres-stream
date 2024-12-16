package postgres

import "github.com/jackc/pglogrepl"

var _ Event = XLogDataEvent{}

type XLogDataEvent pglogrepl.XLogData

// ByteID implements Event.
func (e XLogDataEvent) ByteID() byte {
	return pglogrepl.XLogDataByteID
}
