package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func PeekReplicationSlotConfirmedFlushLSN(ctx context.Context, conn *pgconn.PgConn, slotName string) (lsn pglogrepl.LSN, err error) {
	param, err := conn.EscapeString(slotName)
	if err != nil {
		return
	}

	sql := fmt.Sprintf(__SQL_REPLICATION_SLOT_CONFIRMED_FLUSH_LSN, param)
	reader := conn.Exec(ctx, sql)
	result, err := reader.ReadAll()
	if err != nil {
		return
	}
	for _, r := range result {
		if len(r.Rows) > 0 {
			lsn.Scan(string(r.Rows[0][0]))
			return
		}
	}
	return
}
