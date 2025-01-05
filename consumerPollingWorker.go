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
		xLogPos  pglogrepl.LSN
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
			if !w.handleError(err) {
				w.Logger.Fatalf("%% Error: %v\n", err)
				break
			}
		}
		if msg == nil {
			// ignore all invalid messages
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				panic(err)
			}

			// update XLogPos
			if pkm.ServerWALEnd > xLogPos {
				xLogPos = pkm.ServerWALEnd
			}
			// if pkm.ReplyRequested {
			// 	deadline = time.Time{}
			// }

			ev := PrimaryKeepaliveMessageEvent(pkm)
			w.handleEvent(&ev)

			if !w.AutoAck || xLogPos < pkm.ServerWALEnd {
				continue
			}

			// ack
			if err = consumer.doAck(xLogPos); err != nil {
				if !w.handleError(err) {
					w.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", w.Slot, xLogPos, err)
				}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				if !w.handleError(err) {
					w.Logger.Fatalf("%% Error: %v\n", err)
					break
				}
			}

			// update XLogPos
			if xld.WALStart > xLogPos {
				xLogPos = xld.WALStart
			}

			ev := XLogDataEvent(xld)
			w.handleEvent(&ev)

			err = w.handleMessage(xLogPos, xld)
			if err != nil {
				if !w.handleError(err) {
					w.Logger.Fatalf("%% Error: %v\n", err)
					break
				}
			}

			if !w.AutoAck || xLogPos < xld.WALStart {
				continue
			}

			// ack
			if err = consumer.doAck(xLogPos); err != nil {
				if !w.handleError(err) {
					w.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", w.Slot, xLogPos, err)
				}
			}
		default:
			// do nothing
		}
	}
}

func (w *consumerPollingWorker) handleMessage(xLogPos pglogrepl.LSN, data pglogrepl.XLogData) error {
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

		return w.MessageHandler(&msg)
	}
	return nil
}

func (w *consumerPollingWorker) handleEvent(event Event) {
	if w.EventHandler != nil {
		w.consumer.wg.Add(1)
		defer w.consumer.wg.Done()

		w.EventHandler(event)
	}
}

func (w *consumerPollingWorker) handleError(err error) (disposed bool) {
	if w.EventHandler != nil {
		w.consumer.wg.Add(1)
		defer w.consumer.wg.Done()

		return w.ErrorHandler(err)
	}
	return false
}
