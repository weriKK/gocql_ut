package storage

import (
	"fmt"

	"github.com/gocql/gocql"
)

type StorageService interface {
	Save(u User) error
	Get(name string) (*User, error)
}

type User struct {
	Name string
	Age  int
}

type cassandraService struct {
	session SessionWrapper
	config  CassandraServiceConfig
}

type CassandraServiceConfig struct {
	Host        string
	Port        int
	User        string
	Password    string
	Keyspace    string
	Consistency string
}

type SessionBuilder interface {
	build(cfg CassandraServiceConfig) (SessionWrapper, error)
}

type CassandraSessionBuilder struct{}

func (csb CassandraSessionBuilder) build(cfg CassandraServiceConfig) (SessionWrapper, error) {

	cluster := configureCluster(cfg)

	session, err := cluster.CreateSession()
	return NewCassandraSession(session), err
}

func configureCluster(cfg CassandraServiceConfig) *gocql.ClusterConfig {
	cluster := gocql.NewCluster()
	cluster.Hosts = []string{cfg.Host}
	cluster.Port = cfg.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.User,
		Password: cfg.Password,
	}
	cluster.Keyspace = cfg.Keyspace

	return cluster
}

func NewCassandraService(cfg CassandraServiceConfig, sessBuilder SessionBuilder) (StorageService, error) {

	session, err := sessBuilder.build(cfg)
	if err != nil {
		return nil, fmt.Errorf("NewCassandraService() failed: %s", err)
	}

	return &cassandraService{
		session: session,
		config:  cfg,
	}, nil
}

func (cs *cassandraService) Save(u User) error {
	query := "INSERT INTO kovatable (name, age) VALUES(?,?)"
	if err := cs.session.Query(query, u.Name, u.Age).Exec(); err != nil {
		return fmt.Errorf("Save() failed: %s", err)
	}
	return nil
}

func (cs *cassandraService) Get(name string) (*User, error) {
	var u User
	query := "SELECT name, age FROM kovatable LIMIT 1"
	if err := cs.session.Query(query).Scan(&u.Name, &u.Age); err != nil {
		return nil, fmt.Errorf("Get() failed: %s", err)
	}
	return &u, nil
}
