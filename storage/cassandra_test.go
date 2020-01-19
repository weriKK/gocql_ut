package storage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/mock"
)

type mockSessionBuilder struct {
	mock.Mock
}

func (sb *mockSessionBuilder) build(cfg CassandraServiceConfig) (SessionWrapper, error) {
	retVal := sb.Called(cfg)
	if retVal.Error(0) != nil {
		return nil, retVal.Error(0)
	}

	return &MockSession{mockBuilder: sb}, nil
}

type MockSession struct {
	mockBuilder *mockSessionBuilder
}

func (s *MockSession) Query(stmt string, values ...interface{}) QueryWrapper {
	return &MockQuery{mockSession: s}
}

type MockQuery struct {
	mockSession *MockSession
}

func (q *MockQuery) Scan(dest ...interface{}) error {
	retVal := q.mockSession.mockBuilder.Called(dest)

	retDestCount := len(retVal) - 1
	for i := 0; i < retDestCount; i++ {
		d := reflect.ValueOf(dest[i])
		d.Elem().Set(reflect.ValueOf(retVal.Get(i)))
	}

	retErrorIdx := len(retVal) - 1
	return retVal.Error(retErrorIdx)
}

func (q *MockQuery) Exec() error {
	retVal := q.mockSession.mockBuilder.Called()
	return retVal.Error(0)
}

func TestConfigureCluster(t *testing.T) {
	cfg := CassandraServiceConfig{
		Host:        "funny.hostname",
		Port:        666,
		User:        "cassandra",
		Password:    "cassandra",
		Keyspace:    "testkeyspace",
		Consistency: "LOCAL_QUORUM",
	}

	pwdAuthenticator := gocql.PasswordAuthenticator{
		Username: cfg.User,
		Password: cfg.Password,
	}

	cluster := configureCluster(cfg)

	if !reflect.DeepEqual(cluster.Authenticator, pwdAuthenticator) {
		t.Fatal("cluster.Authenticator is incorrect")
	}
}

func TestNewCassandraService_OK(t *testing.T) {
	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Once().Return(nil)

	_, err := NewCassandraService(CassandraServiceConfig{
		Host:        "666.666.666.666",
		Port:        32769,
		User:        "cassandra",
		Password:    "cassandra",
		Keyspace:    "kovakeyspace",
		Consistency: "LOCAL_QUORUM",
	},
		&msb,
	)

	if err != nil {
		t.Fatal(err)
	}
}

func TestNewCassandraService_Fail(t *testing.T) {
	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Once().Return(fmt.Errorf("Intentional mocked error"))

	_, err := NewCassandraService(CassandraServiceConfig{
		Host:        "666.666.666.666",
		Port:        32769,
		User:        "cassandra",
		Password:    "cassandra",
		Keyspace:    "kovakeyspace",
		Consistency: "LOCAL_QUORUM",
	},
		&msb,
	)

	if err == nil {
		t.Fatal(err)
	}
}

func TestCassandraService_Save_OK(t *testing.T) {

	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Return(nil)
	msb.On("Exec").Once().Return(nil)

	db, err := NewCassandraService(CassandraServiceConfig{}, &msb)
	if err != nil {
		t.Fatal(err)
	}

	user := User{Name: "kova", Age: 35}

	err = db.Save(user)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCassandraService_Save_Fail(t *testing.T) {

	expectedError := fmt.Errorf("Save() failed: Intentional mocked error")

	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Return(nil)
	msb.On("Exec").Once().Return(fmt.Errorf("Intentional mocked error"))

	db, err := NewCassandraService(CassandraServiceConfig{}, &msb)
	if err != nil {
		t.Fatal(err)
	}

	user := User{Name: "kova", Age: 35}

	err = db.Save(user)
	if err == nil || err.Error() != expectedError.Error() {
		t.Fatalf("Unexpected error: Wanted: '%s', Got: '%s'", expectedError.Error(), err.Error())
	}
}

func TestCassandraService_Get_OK(t *testing.T) {

	expectedUser := &User{Name: "kova", Age: 35}

	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Return(nil)
	msb.On("Scan", mock.Anything).Once().Return(expectedUser.Name, expectedUser.Age, nil)

	db, err := NewCassandraService(CassandraServiceConfig{}, &msb)
	if err != nil {
		t.Fatal(err)
	}

	user, err := db.Get(expectedUser.Name)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(user, expectedUser) {
		t.Fatalf("Returned incorrect User! Wanted: %#v, Got: %#v", expectedUser, user)

	}
}

func TestCassandraService_Get_Fail(t *testing.T) {

	expectedError := fmt.Errorf("Get() failed: Intentional mock error")

	msb := mockSessionBuilder{}
	msb.On("build", mock.Anything).Return(nil)
	msb.On("Scan", mock.Anything).Once().Return(fmt.Errorf("Intentional mock error"))

	db, err := NewCassandraService(CassandraServiceConfig{}, &msb)
	if err != nil {
		t.Fatal(err)
	}

	user, err := db.Get("anyString")
	if err == nil || err.Error() != expectedError.Error() {
		t.Fatalf("Unexpected error: Wanted: '%s', Got: '%s'", expectedError.Error(), err.Error())
	}

	if user != nil {
		t.Fatalf("Returned incorrect User{}! Wanted: %#v, Got: %#v", nil, user)
	}
}
