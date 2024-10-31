package db

import (
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlserver"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
)

func RunMigrationsUp(db *sqlx.DB, dbName string) error {
	driver, err := sqlserver.WithInstance(db.DB, &sqlserver.Config{})
	if err != nil {
		return err
	}
	defer driver.Close()

	m, err := migrate.NewWithDatabaseInstance("file://db/migrations", dbName, driver)
	if err != nil {
		return err
	}

	return m.Up()
}
