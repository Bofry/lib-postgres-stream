package postgres

import "github.com/jackc/pglogrepl"

type ReplicationOptions []ReplicationOption

func ConfigureReplicationOptions() ReplicationOptions {
	return ReplicationOptions(nil)
}

func (opt ReplicationOptions) WithPluginArgs(args ...string) ReplicationOptions {
	return append(opt, WithPluginArgs(args...))
}

func (opt ReplicationOptions) WithReplicationMode(mode pglogrepl.ReplicationMode) ReplicationOptions {
	return append(opt, WithReplicationMode(mode))
}
