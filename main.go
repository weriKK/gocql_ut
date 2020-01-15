package main

import "log"

import "casswrap/storage"

func main() {
	db, err := storage.NewCassandraService(storage.CassandraServiceConfig{
		Host:        "192.168.1.114",
		Port:        32774,
		User:        "cassandra",
		Password:    "cassandra",
		Keyspace:    "kovakeyspace",
		Consistency: "LOCAL_QUORUM",
	},
		storage.CassandraSessionBuilder{},
	)

	if err != nil {
		log.Fatal(err)
	}

	err = db.Save(storage.User{
		Name: "kova",
		Age:  35,
	})
	if err != nil {
		log.Fatal(err)
	}

	user, err := db.Get("kova")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%#v", user)

}
