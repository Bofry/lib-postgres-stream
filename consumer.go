package postgres

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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

	conn  *pgconn.PgConn
	slots map[string]ReplicationSlotSource
	wg    sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     atomic.Bool
	disposed    atomic.Bool
	pausing     atomic.Bool
}

func (c *Consumer) Subscribe(slots ...SlotOffsetInfo) (err error) {
	c.mutex.Lock()

	if c.disposed.Load() {
		c.mutex.Unlock()
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running.Load() {
		c.mutex.Unlock()
		return fmt.Errorf("the Consumer is running")
	}

	defer func() {
		if err != nil {
			c.running.Store(false)
			c.disposed.Store(true)
		}
		c.mutex.Unlock()

		// Close() short-circuits on disposed, so the connection has to be
		// released here. Do it after the mutex is dropped: wg.Wait() blocks
		// on in-flight message handlers, which run for an unbounded time and
		// may themselves call Close(). Waiting for them under the lock would
		// deadlock against Close()'s own mutex acquisition.
		if err != nil {
			c.wg.Wait()
			if c.conn != nil {
				// the connection is not safe for concurrent use, so this must
				// follow wg.Wait() rather than run alongside live workers.
				c.conn.Close(context.Background())
			}
		}
	}()

	c.init()
	c.running.Store(true)
	c.pausing.Store(false)

	// new slots
	c.slots = make(map[string]ReplicationSlotSource)

	// new conn
	{
		conn, cerr := NewConn(c.Config)
		if cerr != nil {
			return cerr
		}

		c.conn = conn
	}

	return c.subscribe(slots...)
}

func (c *Consumer) Close() {
	if c.disposed.Load() {
		return
	}

	c.mutex.Lock()
	if c.disposed.Load() {
		c.mutex.Unlock()
		return
	}
	c.running.Store(false)
	c.disposed.Store(true)
	c.mutex.Unlock()

	c.wg.Wait()

	if c.conn != nil {
		c.conn.Close(context.Background())
	}
}

func (c *Consumer) Pause() {
	c.pausing.Store(true)
}

func (c *Consumer) Resume() {
	c.pausing.Store(false)
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.Config == nil {
		c.Config = new(Config)
	}

	if c.Logger == nil {
		c.Logger = defaultLogger
	}

	c.initialized = true
}

func (c *Consumer) doAck(xLogPos pglogrepl.LSN) error {
	if c.disposed.Load() {
		return nil
	}
	if !c.running.Load() {
		return nil
	}

	return pglogrepl.SendStandbyStatusUpdate(context.Background(),
		c.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: xLogPos,
		})
}

func (c *Consumer) subscribe(slots ...SlotOffsetInfo) error {
	if len(slots) == 0 {
		return nil
	}

	var (
		sysident  pglogrepl.IdentifySystemResult
		slotnames []string = make([]string, len(slots))

		conn = c.conn
	)

	for i, v := range slots {
		slotnames[i] = v.getSlotOffset().Slot
	}

	// get system info
	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return err
	}
	c.Logger.Println(
		"SystemID:", sysident.SystemID,
		"Timeline:", sysident.Timeline,
		"XLogPos:", sysident.XLogPos,
		"DBName:", sysident.DBName)

	// get slot info
	slotRecords, err := SelectReplicationSlot(context.Background(), conn, slotnames)
	if err != nil {
		return err
	}
	for _, r := range slotRecords {
		c.slots[r.SlotName] = r
	}

	// update startLSN for all slots
	for _, info := range slots {
		var (
			slot   = info.getSlotOffset()
			source = c.slots[slot.Slot]
		)

		switch slot.LSN {
		case StreamUnspecifiedOffset:
			source.startLSN = source.ConfirmedFlushLSN
		case StreamZeroOffset:
			source.startLSN = pglogrepl.LSN(0)
		case StreamNeverDeliveredOffset:
			source.startLSN = sysident.XLogPos
		default:
			lsn, err := pglogrepl.ParseLSN(slot.LSN)
			if err != nil {
				return err
			}
			source.startLSN = lsn
		}
		c.slots[slot.Slot] = source
	}

	var options = pglogrepl.StartReplicationOptions{}
	for _, opt := range c.Config.ReplicationOptions {
		opt.applyStartReplicationOptions(&options)
	}

	// start event loop
	for slot, source := range c.slots {
		c.Logger.Printf("StartReplication:: %+v", source)
		err = pglogrepl.StartReplication(context.Background(), c.conn,
			slot,
			source.startLSN,
			options)
		if err != nil {
			return err
		}

		worker := &consumerPollingWorker{
			consumer:       c,
			Slot:           slot,
			DBName:         sysident.DBName,
			SystemID:       sysident.SystemID,
			MessageHandler: c.MessageHandler,
			EventHandler:   c.EventHandler,
			ErrorHandler:   c.ErrorHandler,
			Logger:         c.Logger,
			lastFlushLSN:   source.startLSN,
		}

		// event loop
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			worker.run(c.Config.PollingTimeout)
		}()
	}
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
