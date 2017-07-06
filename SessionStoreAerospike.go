package aerospikestore

import (
	"errors"

	"github.com/aerogo/aerospike"
	"github.com/aerogo/session"
	as "github.com/aerospike/aerospike-client-go"
)

// SessionStoreAerospike is a store saving sessions in an Aerospike database.
type SessionStoreAerospike struct {
	db          *aerospike.Database
	set         string
	writePolicy *as.WritePolicy

	// Session duration in seconds (a.k.a. TTL).
	duration int
}

// New creates a session store using an Aerospike database.
func New(database *aerospike.Database, set string, duration int) *SessionStoreAerospike {
	writePolicy := as.NewWritePolicy(0, uint32(duration))
	writePolicy.RecordExistsAction = as.REPLACE

	return &SessionStoreAerospike{
		db:          database,
		set:         set,
		duration:    duration,
		writePolicy: writePolicy,
	}
}

// Get loads the initial session values from the database.
func (store *SessionStoreAerospike) Get(sid string) (*session.Session, error) {
	key, _ := as.NewKey(store.db.Namespace(), store.set, sid)
	record, err := store.db.Client.Get(nil, key)

	if err != nil {
		return nil, err
	}

	if record == nil {
		return nil, errors.New("Record is nil (session ID: " + sid + ")")
	}

	return session.New(sid, record.Bins), nil
}

// Set updates the session values in the database.
func (store *SessionStoreAerospike) Set(sid string, session *session.Session) error {
	sessionData := session.Data()
	key, _ := as.NewKey(store.db.Namespace(), store.set, sid)

	// Set with nil as data means we should delete the session.
	if sessionData == nil {
		_, err := store.db.Client.Delete(nil, key)
		return err
	}

	return store.db.Client.Put(store.writePolicy, key, sessionData)
}
