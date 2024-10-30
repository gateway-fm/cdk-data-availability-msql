package msql

import (
	"context"
	"fmt"

	"github.com/0xPolygon/cdk-data-availability/db"
	"github.com/0xPolygon/cdk-data-availability/log"
	"github.com/jmoiron/sqlx"
)

// InitContext initializes DB connection by the given config
func InitContext(ctx context.Context, cfg db.Config) (*sqlx.DB, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
		cfg.Host, cfg.User, cfg.Password, cfg.Port, cfg.Name)

	conn, err := sqlx.ConnectContext(ctx, "sqlserver", connString)
	if err != nil {
		log.Errorf("Unable to connect to database: %v\n", err)
		return nil, err
	}

	conn.DB.SetMaxIdleConns(cfg.MaxConns)

	if err = conn.PingContext(ctx); err != nil {
		log.Errorf("Unable to ping the database: %v\n", err)
		return nil, err
	}

	return conn, nil
}
