package storage

import "github.com/gocql/gocql"

// SESSION
//---------------------------------------------------------------------------------------------

type SessionWrapper interface {
	Query(stmt string, values ...interface{}) QueryWrapper
}

type CassandraSession struct {
	session *gocql.Session
}

func NewCassandraSession(session *gocql.Session) SessionWrapper {
	return &CassandraSession{session}
}

func (s *CassandraSession) Query(stmt string, values ...interface{}) QueryWrapper {
	return NewCassandraQuery(s.session.Query(stmt, values...))
}

// Query
//---------------------------------------------------------------------------------------------

type QueryWrapper interface {
	Scan(dest ...interface{}) error
	Exec() error
}

type CassandraQuery struct {
	query *gocql.Query
}

func NewCassandraQuery(query *gocql.Query) QueryWrapper {
	return &CassandraQuery{query}
}

func (q *CassandraQuery) Scan(dest ...interface{}) error {
	return q.query.Scan(dest...)
}

func (q *CassandraQuery) Exec() error {
	return q.query.Exec()
}
