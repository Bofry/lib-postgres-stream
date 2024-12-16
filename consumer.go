package postgres

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Consumer struct {
	MessageHandler MessageHandleProc
	EventHandler   EventHandleProc
	ErrorHandler   ErrorHandleProc
	Logger         *log.Logger
	Config         *Config

	DisableAutoAck bool

	conn *pgconn.PgConn
	wg   sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(slot SlotOffsetInfo) error {
	return c.subscribe(slot.getSlotOffset())
}

func (c *Consumer) Close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	c.running = false

	defer func() {
		c.disposed = true
		// dispose
		c.mutex.Unlock()
	}()

	c.wg.Wait()

	c.conn.Close(context.Background())
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.Config == nil {
		c.Config = NewConfig()
	}

	if c.Logger == nil {
		c.Logger = defaultLogger
	}

	c.initialized = true
}

func (c *Consumer) doAck(xLogPos pglogrepl.LSN) error {
	if c.disposed {
		return nil
	}
	if !c.running {
		return nil
	}

	return pglogrepl.SendStandbyStatusUpdate(context.Background(),
		c.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: xLogPos,
		})
}

func (c *Consumer) subscribe(slot SlotOffset) error {
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.init()
	c.running = true

	// new conn
	{
		conn, err := c.createConn()
		if err != nil {
			return err
		}

		c.conn = conn
	}

	// start LSN
	var (
		sysident pglogrepl.IdentifySystemResult
		startLSN pglogrepl.LSN

		conn = c.conn
	)
	{
		sysident, err = pglogrepl.IdentifySystem(context.Background(), conn)
		if err != nil {
			return err
		}

		switch slot.LSN {
		case StreamUnspecifiedOffset:
			lsn, err := PeekReplicationSlotConfirmedFlushLSN(context.Background(), conn, slot.SlotName)
			if err != nil {
				return err
			}
			startLSN = lsn

			if startLSN == pglogrepl.LSN(0) {
				startLSN = sysident.XLogPos
			}
		case StreamZeroOffset:
			startLSN = pglogrepl.LSN(0)
		case StreamNeverDeliveredOffset:
			startLSN = sysident.XLogPos
		default:
			lsn, err := pglogrepl.ParseLSN(slot.LSN)
			if err != nil {
				return err
			}
			startLSN = lsn
		}

		c.Logger.Println(
			"SystemID:", sysident.SystemID,
			"Timeline:", sysident.Timeline,
			"XLogPos:", sysident.XLogPos,
			"DBName:", sysident.DBName,
			"StartLSN:", startLSN)
	}

	var options = pglogrepl.StartReplicationOptions{}
	for _, opt := range c.Config.ReplicationOptions {
		opt.applyStartReplicationOptions(&options)
	}
	err = pglogrepl.StartReplication(context.Background(), c.conn,
		slot.SlotName,
		startLSN,
		options)
	if err != nil {
		return err
	}

	// event loop
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		var (
			timeout  = c.Config.PollingTimeout
			deadline time.Time

			clientXLogPos pglogrepl.LSN
		)

		for c.running {
			deadline = time.Now().Add(timeout)

			msg, err := c.read(deadline)
			if err != nil {
				// ignore any error if disposed or not running
				if !c.running {
					break
				}
				if pgconn.Timeout(err) {
					continue
				}
				if !c.handleError(err) {
					c.Logger.Fatalf("%% Error: %v\n", err)
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

				// update clientXLogPos
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
				// if pkm.ReplyRequested {
				// 	deadline = time.Time{}
				// }

				ev := PrimaryKeepaliveMessageEvent(pkm)
				c.handleEvent(&ev)

				// ack
				if err = c.doAck(clientXLogPos); err != nil {
					if !c.handleError(err) {
						c.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", slot.SlotName, clientXLogPos, err)
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					if !c.handleError(err) {
						c.Logger.Fatalf("%% Error: %v\n", err)
						break
					}
				}

				// update clientXLogPos
				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				ev := XLogDataEvent(xld)
				c.handleEvent(&ev)

				err = c.handlerMessae(slot.SlotName, clientXLogPos, xld)
				if err != nil {
					if !c.handleError(err) {
						c.Logger.Fatalf("%% Error: %v\n", err)
						break
					}
				}

				if c.DisableAutoAck {
					continue
				}

				// ack
				if err = c.doAck(clientXLogPos); err != nil {
					if !c.handleError(err) {
						c.Logger.Printf("SendStandbyStatusUpdate failed on (%s#%s): %+v", slot.SlotName, clientXLogPos, err)
					}
				}
			default:
				// do nothing
			}
		}
	}()

	return nil
}

func (c *Consumer) read(deadline time.Time) (*pgproto3.CopyData, error) {
	var (
		conn = c.conn
	)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	rawMsg, err := conn.ReceiveMessage(ctx)
	cancel()
	if err != nil {
		return nil, err
	}
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("received Postgres WAL error: %+v", errMsg)
	}
	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, nil
	}
	return msg, nil
}

func (c *Consumer) handleEvent(event Event) {
	if c.EventHandler != nil {
		c.EventHandler(event)
	}
}

func (c *Consumer) handlerMessae(slotName string, consumedXLogPos pglogrepl.LSN, data pglogrepl.XLogData) error {
	if c.MessageHandler != nil {
		c.wg.Add(1)
		defer c.wg.Done()

		msg := Message{
			SlotName:        slotName,
			Delegate:        &clientMessageDelegate{client: c},
			consumedXLogPos: consumedXLogPos,
			data:            &data,
		}

		return c.MessageHandler(&msg)
	}
	return nil
}

func (c *Consumer) handleError(err error) (disposed bool) {
	if c.EventHandler != nil {
		c.wg.Add(1)
		defer c.wg.Done()
		return c.ErrorHandler(err)
	}
	return false
}

func (c *Consumer) createConn() (*pgconn.PgConn, error) {
	c.Config.init()

	config, err := pgconn.ParseConfig(fmt.Sprintf("postgres://%s?replication=database", c.Config.Host))
	if err != nil {
		panic(err)
	}
	config.Port = c.Config.Port
	config.User = c.Config.User
	config.Password = c.Config.Password
	config.Database = c.Config.Database
	config.ConnectTimeout = c.Config.ConnectTimeout

	return pgconn.ConnectConfig(context.Background(), config)
}
