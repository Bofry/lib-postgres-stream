package postgres

import (
	"github.com/jackc/pglogrepl"
)

var _ Event = PrimaryKeepaliveMessageEvent{}

type PrimaryKeepaliveMessageEvent pglogrepl.PrimaryKeepaliveMessage

// ByteID implements Event.
func (e PrimaryKeepaliveMessageEvent) ByteID() byte {
	return pglogrepl.PrimaryKeepaliveMessageByteID
}
