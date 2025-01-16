package postgres

import (
	"sync/atomic"
	"time"

	"github.com/Bofry/trace"
	"github.com/jackc/pglogrepl"
)

var (
	_ trace.TracerTagMarshaler = new(Message)
)

type Message struct {
	Slot     string
	Delegate MessageDelegate

	consumedXLogPos pglogrepl.LSN
	data            *pglogrepl.XLogData
	database        string
	systemID        string

	responded int32
}

func (m *Message) Ack() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnAck(m)
}

func (m *Message) SystemID() string {
	return m.systemID
}

func (m *Message) Database() string {
	return m.database
}

func (m *Message) StartLSN() LSN {
	return m.data.WALStart
}

func (m *Message) Timestamp() time.Time {
	return m.data.ServerTime
}

func (m *Message) Body() []byte {
	return m.data.WALData
}

func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

func (m *Message) Clone() *Message {
	cloned := *m
	return &cloned
}

// MarshalTracerTag implements trace.TracerTagMarshaler.
func (m *Message) MarshalTracerTag(builder *trace.TracerTagBuilder) error {
	builder.String("systen_id", m.SystemID())
	builder.String("database", m.Database())
	builder.String("lsn", m.StartLSN().String())
	builder.String("timestamp", m.Timestamp().UTC().String())
	builder.String("slot", m.Slot)
	builder.String("body", string(m.Body()))
	return nil
}
