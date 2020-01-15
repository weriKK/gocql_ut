package storage

import "testing"

func TestNewCassandraService(t *testing.T) {
	_, err := NewCassandraService(CassandraServiceConfig{
		Host:        "192.168.1.200",
		Port:        32774,
		User:        "cassandra",
		Password:    "cassandra",
		Keyspace:    "kovakeyspace",
		Consistency: "LOCAL_QUORUM",
	},
		CassandraSessionBuilder{},
	)

	if err != nil {
		t.Fatal(err)
	}
}
