package postgres

import (
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type consumerPollingWorker struct {
	consumer *Consumer

	Slot     string
	DBName   string
	SystemID string
	AutoAck  bool

	MessageHandler MessageHandleProc
	EventHandler   EventHandleProc
	ErrorHandler   ErrorHandleProc
	Logger         *log.Logger
}

func (w *consumerPollingWorker) run(timeout time.Duration) {
	var (
		consumer = w.consumer
		deadline time.Time
	)

	for consumer.running {
		deadline = time.Now().Add(timeout)

		msg, err := consumer.read(deadline)
		if err != nil {
			// ignore any error if disposed or not running
			if !consumer.running {
				break
			}
			if pgconn.Timeout(err) {
				continue
			}
			if !w.processError(err) {
				w.Logger.Fatalf("%% Error: %v\n", err)
				continue
			}
		}

		if msg == nil {
			// ignore all invalid messages
			continue
		}

		w.processData(msg.Data)
	}
}

func (w *consumerPollingWorker) processData(data []byte) {
	var (
		consumer = w.consumer
		xLogPos  pglogrepl.LSN
	)

	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			if !w.processError(err) {
				w.Logger.Printf("ParsePrimaryKeepaliveMessage() failed on (%s#%s): %+v", w.Slot, xLogPos, err)
			}
			break
		}

		// update XLogPos
		if pkm.ServerWALEnd > xLogPos {
			xLogPos = pkm.ServerWALEnd
		}
		// if pkm.ReplyRequested {
		// 	deadline = time.Time{}
		// }

		ev := PrimaryKeepaliveMessageEvent(pkm)
		w.processEvent(&ev)

		if !w.AutoAck || xLogPos < pkm.ServerWALEnd {
			break
		}

		// ack
		if err = consumer.doAck(xLogPos); err != nil {
			if !w.processError(err) {
				w.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", w.Slot, xLogPos, err)
			}
			break
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			if !w.processError(err) {
				w.Logger.Printf("%% Error: %v\n", err)
			}
			break
		}

		// update XLogPos
		if xld.WALStart > xLogPos {
			xLogPos = xld.WALStart
		}

		ev := XLogDataEvent(xld)
		w.processEvent(&ev)
		w.processMessage(xLogPos, xld)

		if !w.AutoAck || xLogPos < xld.WALStart {
			break
		}

		// ack
		if err = consumer.doAck(xLogPos); err != nil {
			if !w.processError(err) {
				w.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", w.Slot, xLogPos, err)
			}
			break
		}
	default:
		// do nothing
	}
}

func (w *consumerPollingWorker) processMessage(xLogPos pglogrepl.LSN, data pglogrepl.XLogData) {
	if w.MessageHandler != nil {
		w.consumer.wg.Add(1)
		defer w.consumer.wg.Done()

		msg := Message{
			Slot:            w.Slot,
			Delegate:        &clientMessageDelegate{client: w.consumer},
			consumedXLogPos: xLogPos,
			data:            &data,
			database:        w.DBName,
			systemID:        w.SystemID,
		}

		w.MessageHandler(&msg)
	}
}

func (w *consumerPollingWorker) processEvent(event Event) {
	if w.EventHandler != nil {
		w.consumer.wg.Add(1)
		defer w.consumer.wg.Done()

		w.EventHandler(event)
	}
}

func (w *consumerPollingWorker) processError(err error) (disposed bool) {
	if w.EventHandler != nil {
		w.consumer.wg.Add(1)
		defer w.consumer.wg.Done()

		return w.ErrorHandler(err)
	}
	return false
}
