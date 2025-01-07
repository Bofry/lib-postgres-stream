package postgres

type ReplicationOptions []ReplicationOption

func ConfigureReplicationOptions() ReplicationOptions {
	return ReplicationOptions(nil)
}

func (opt ReplicationOptions) WithPluginArgs(args ...string) ReplicationOptions {
	return append(opt, WithPluginArgs(args...))
}
