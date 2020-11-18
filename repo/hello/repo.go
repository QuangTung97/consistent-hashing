package hello

import (
	"sharding/domain/hello"

	"github.com/jmoiron/sqlx"
)

// Repo for hello repository
type Repo struct {
	db *sqlx.DB
}

var _ hello.IRepository = &Repo{}

// NewRepo creates a Repo
func NewRepo(db *sqlx.DB) *Repo {
	return &Repo{
		db: db,
	}
}
