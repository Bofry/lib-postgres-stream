package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func ParseReplicationMode(s string) (ReplicationMode, error) {
	switch strings.ToUpper(s) {
	case LogicalReplication.String():
		return LogicalReplication, nil
	case PhysicalReplication.String():
		return PhysicalReplication, nil
	}
	return 0, fmt.Errorf("unsupported slot type '%s'", s)
}

func SelectReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slots []string) (records []ReplicationSlotSource, err error) {
	if len(slots) == 0 {
		return
	}

	var slotParam []string = make([]string, len(slots))
	for i, slot := range slots {
		param, err := conn.EscapeString(slot)
		if err != nil {
			return nil, err
		}
		slotParam[i] = "'" + param + "'"
	}

	sql := fmt.Sprintf(__SQL_SELECT_REPLICATION_SLOT, strings.Join(slotParam, ","))
	reader := conn.Exec(ctx, sql)
	result, err := reader.ReadAll()
	if err != nil {
		return
	}
	for _, r := range result {
		if len(r.Rows) > 0 {
			records = make([]ReplicationSlotSource, len(r.Rows))
			for i, v := range r.Rows {
				r := ReplicationSlotSource{
					SlotName: string(v[0]),
					Plugin:   string(v[1]),
					Database: string(v[3]),
				}
				{
					t, err := ParseReplicationMode(string(v[2]))
					if err != nil {
						return nil, err
					}
					r.SlotType = t
				}
				{
					b, err := strconv.ParseBool(string(v[4]))
					if err != nil {
						return nil, err
					}
					r.Temporary = b
				}
				{
					b, err := strconv.ParseBool(string(v[5]))
					if err != nil {
						return nil, err
					}
					r.Active = b
				}
				r.RestartLSN.Scan(string(v[6]))
				r.ConfirmedFlushLSN.Scan(string(v[7]))

				records[i] = r
			}
			return
		}
	}
	return
}

func IsDuplicateObjectError(err error) bool {
	if verr, ok := err.(*pgconn.PgError); ok {
		return verr.Code == __PG_ERRCODE_DUPLICATE_OBJECT
	}
	return false
}

func CreateReplicationSlot(ctx context.Context, conn *pgconn.PgConn, provider CreateReplicationSlotSourceProvider) error {
	for _, source := range provider.sources {
		_, err := pglogrepl.CreateReplicationSlot(ctx, conn,
			source.SlotName,
			source.Plugin,
			pglogrepl.CreateReplicationSlotOptions{
				Temporary: source.Temporary,
				Mode:      source.SlotType,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func NewConn(config *Config) (*pgconn.PgConn, error) {
	config.init()

	c, err := pgconn.ParseConfig(fmt.Sprintf("postgres://%s?replication=database", config.Host))
	if err != nil {
		return nil, err
	}
	c.Port = config.Port
	c.User = config.User
	c.Password = config.Password
	c.Database = config.Database
	c.ConnectTimeout = config.ConnectTimeout

	return pgconn.ConnectConfig(context.Background(), c)
}
