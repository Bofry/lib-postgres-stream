package postgres

import "github.com/jackc/pglogrepl"

var _ ReplicationOption = StartReplicationOptionsFunc(nil)

type StartReplicationOptionsFunc func(opt *pglogrepl.StartReplicationOptions)

// applyStartReplicationOptions implements ReplicationOption.
func (fn StartReplicationOptionsFunc) applyStartReplicationOptions(opt *pglogrepl.StartReplicationOptions) {
	fn(opt)
}

// /////////////////////////////////
func WithPluginArgs(args ...string) ReplicationOption {
	return StartReplicationOptionsFunc(func(opt *pglogrepl.StartReplicationOptions) {
		opt.PluginArgs = args
	})
}

// /////////////////////////////////
func WithReplicationMode(mode pglogrepl.ReplicationMode) ReplicationOption {
	return StartReplicationOptionsFunc(func(opt *pglogrepl.StartReplicationOptions) {
		opt.Mode = mode
	})
}
