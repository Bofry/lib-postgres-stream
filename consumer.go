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
	AutoCreateSlot bool // 自動註冊 slot
	DisableAutoAck bool

	conn  *pgconn.PgConn
	slots map[string]ReplicationSlotSource
	wg    sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(slots ...SlotOffsetInfo) error {
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

	// new slots
	c.slots = make(map[string]ReplicationSlotSource)

	// new conn
	{
		conn, err := c.createConn()
		if err != nil {
			return err
		}

		c.conn = conn
	}

	return c.subscribe(slots...)
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
			AutoAck:        !c.DisableAutoAck,
			MessageHandler: c.MessageHandler,
			EventHandler:   c.EventHandler,
			ErrorHandler:   c.ErrorHandler,
			Logger:         c.Logger,
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
