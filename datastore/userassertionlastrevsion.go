package datastore

import (
	"database/sql"

	"github.com/CanonicalLtd/serial-vault/service/log"
)

const defaultUserAssertionRevision = 1

const createUserAssertionLastRevisionTableSQL = `
        CREATE TABLE IF NOT EXISTS lastrevision (
                model_id        varchar(200) not null,
                user_email      varchar(255) not null,
                last_revision   int not null

                CONSTRAINT lastrevision_pkey PRIMARY KEY ("model_id", "user_email")
        );
`

type UserAssertionLastRevisionStore interface {
	GetLastRevision(string, string) (int, error)
	SaveLastRevision(string, string, int) error
}

const getLastRevisionSQL = `
        SELECT last_revision
        FROM lastrevision
        WHERE model_id=$1 AND user_email=$2
`

const upsertLastRevisionSQL = `
        INSERT INTO last_revision (model_id, user_email, last_revision)
        VALUES ($1, $2, $3)
        ON CONFLICT (lastrevision_pkey) DO UPDATE SET last_revision = EXCLUDED.last_revision
`

func (db *DB) CreateLastRevisionTable() error {
	_, err := db.Exec(createUserAssertionLastRevisionTableSQL)
	return err
}

func (db *DB) GetLastRevision(modelId string, userEmail string) (int, error) {
	row, err := db.QueryRow(getLastRevisionSQL, modelId, userEmail)
	if err != nil {
		log.Printf("Error retrieving user's assertion last revision: %v", err)
		return 0, err
	}
	defer row.Close()

	return db.rowToLastRevision(row)
}

func (db *DB) SaveLastRevision(modelId string, userEmail string, lastRevision int) error {
	_, err := db.Exec(upsertLastRevisionSQL, modelId, userEmail, lastRevision)
	if err != nil {
		log.Printf("Error saving a new user's assertion last revision: %v", err)
		return err
	}

	return nil
}

func (db *DB) rowToLastRevision(row *sql.Row) (int, error) {
	// The default is 1 because this was the Serial Vault's hardcoded value
	// up until now
	lastRevision := defaultUserAssertionRevision
	err := row.Scan(&lastRevision)
	return lastRevision, err
}
