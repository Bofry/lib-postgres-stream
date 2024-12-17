package postgres

import (
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
)

type Message struct {
	Slot     string
	Delegate MessageDelegate

	consumedXLogPos pglogrepl.LSN
	data            *pglogrepl.XLogData

	responded int32
}

func (m *Message) Ack() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnAck(m)
}

func (m *Message) StartLSN() LSN {
	return m.data.WALStart
}

func (m *Message) Timestamp() time.Time {
	return m.data.ServerTime
}

func (m *Message) Bytes() []byte {
	return m.data.WALData
}

func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

func (m *Message) Clone() *Message {
	cloned := *m
	return &cloned
}
