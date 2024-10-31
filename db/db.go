package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/0xPolygon/cdk-data-availability/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
)

const (
	// storeLastProcessedBlockSQL is a query that stores the last processed block for a given task
	storeLastProcessedBlockSQL = `
	BEGIN
		MERGE data_node.sync_tasks AS target
		USING (SELECT @Task AS task, @Block AS block) AS source
		ON (target.task = source.task)
		WHEN MATCHED THEN
			-- Update the block and processed fields if the task already exists
			UPDATE SET target.block = source.block, target.processed = GETDATE()
		WHEN NOT MATCHED THEN
			-- Insert a new record if the task does not exist
			INSERT (task, block, processed)
			VALUES (source.task, source.block, GETDATE());
	END;
`

	// getLastProcessedBlockSQL is a query that returns the last processed block for a given task
	getLastProcessedBlockSQL = `SELECT block FROM data_node.sync_tasks WHERE task = @Task;`

	// getUnresolvedBatchKeysSQL is a query that returns the unresolved batch keys from the database
	getUnresolvedBatchKeysSQL = `
		SELECT num, hash FROM data_node.unresolved_batches
		ORDER BY num
		OFFSET 0 ROWS FETCH NEXT @L ROWS ONLY;`

	// getOffchainDataSQL is a query that returns the offchain data for a given key
	getOffchainDataSQL = `
		SELECT TOP 1 [key], value, batch_num
		FROM data_node.offchain_data 
		WHERE [key] = @Key;
	`

	// listOffchainDataSQL is a query that returns the offchain data for a given list of keys
	listOffchainDataSQL = `
		SELECT [key], value, batch_num
		FROM data_node.offchain_data 
		WHERE [key] IN (?);
	`

	// countOffchainDataSQL is a query that returns the count of rows in the offchain_data table
	countOffchainDataSQL = "SELECT COUNT(*) FROM data_node.offchain_data;"

	// selectOffchainDataGapsSQL is a query that returns the gaps in the offchain_data table
	selectOffchainDataGapsSQL = `
		WITH numbered_batches AS (
			SELECT
				batch_num,
				ROW_NUMBER() OVER (ORDER BY batch_num) AS row_number
			FROM data_node.offchain_data
		)
		SELECT
			nb1.batch_num AS current_batch_num,
			nb2.batch_num AS next_batch_num
		FROM
			numbered_batches nb1
				LEFT JOIN numbered_batches nb2 ON nb1.row_number = nb2.row_number - 1
		WHERE
			nb1.batch_num IS NOT NULL
		  AND nb2.batch_num IS NOT NULL
		  AND nb1.batch_num + 1 <> nb2.batch_num;`
)

var (
	// ErrStateNotSynchronized indicates the state database may be empty
	ErrStateNotSynchronized = errors.New("state not synchronized")
)

// DB defines functions that a DB instance should implement
type DB interface {
	StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error
	GetLastProcessedBlock(ctx context.Context, task string) (uint64, error)

	StoreUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error
	GetUnresolvedBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error)
	DeleteUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error

	GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error)
	ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error)
	StoreOffChainData(ctx context.Context, od []types.OffChainData) error
	CountOffchainData(ctx context.Context) (uint64, error)
	DetectOffchainDataGaps(ctx context.Context) (map[uint64]uint64, error)
}

// DB is the database layer of the data node
type msqlDB struct {
	msql *sqlx.DB

	storeLastProcessedBlockStmt *sqlx.Stmt
	getLastProcessedBlockStmt   *sqlx.Stmt
	getUnresolvedBatchKeysStmt  *sqlx.Stmt
	getOffChainDataStmt         *sqlx.Stmt
	countOffChainDataStmt       *sqlx.Stmt
	detectOffChainDataGapsStmt  *sqlx.Stmt
}

// New instantiates a DB
func New(ctx context.Context, msql *sqlx.DB) (DB, error) {
	storeLastProcessedBlockStmt, err := msql.PreparexContext(ctx, storeLastProcessedBlockSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the store last processed block statement: %w", err)
	}

	getLastProcessedBlockStmt, err := msql.PreparexContext(ctx, getLastProcessedBlockSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get last processed block statement: %w", err)
	}

	getUnresolvedBatchKeysStmt, err := msql.PreparexContext(ctx, getUnresolvedBatchKeysSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get unresolved batch keys statement: %w", err)
	}

	getOffChainDataStmt, err := msql.PreparexContext(ctx, getOffchainDataSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get offchain data statement: %w", err)
	}

	countOffChainDataStmt, err := msql.PreparexContext(ctx, countOffchainDataSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the count offchain data statement: %w", err)
	}

	detectOffChainDataGapsStmt, err := msql.PreparexContext(ctx, selectOffchainDataGapsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the detect offchain data gaps statement: %w", err)
	}

	return &msqlDB{
		msql:                        msql,
		storeLastProcessedBlockStmt: storeLastProcessedBlockStmt,
		getLastProcessedBlockStmt:   getLastProcessedBlockStmt,
		getUnresolvedBatchKeysStmt:  getUnresolvedBatchKeysStmt,
		getOffChainDataStmt:         getOffChainDataStmt,
		countOffChainDataStmt:       countOffChainDataStmt,
		detectOffChainDataGapsStmt:  detectOffChainDataGapsStmt,
	}, nil
}

// StoreLastProcessedBlock stores a record of a block processed by the synchronizer for named task
func (db *msqlDB) StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error {
	_, err := db.storeLastProcessedBlockStmt.ExecContext(ctx, sql.Named("Task", task), sql.Named("Block", block))
	return err
}

// GetLastProcessedBlock returns the latest block successfully processed by the synchronizer for named task
func (db *msqlDB) GetLastProcessedBlock(ctx context.Context, task string) (uint64, error) {
	var lastBlock uint64

	if err := db.getLastProcessedBlockStmt.QueryRowContext(ctx, sql.Named("Task", task)).Scan(&lastBlock); err != nil {
		return 0, err
	}

	return lastBlock, nil
}

// StoreUnresolvedBatchKeys stores unresolved batch keys in the database
func (db *msqlDB) StoreUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	if len(bks) == 0 {
		return nil
	}

	query, args := buildBatchKeysInsertQuery(bks)

	if _, err := db.msql.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to store unresolved batches: %w", err)
	}

	return nil
}

// GetUnresolvedBatchKeys returns the unresolved batch keys from the database
func (db *msqlDB) GetUnresolvedBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error) {
	rows, err := db.getUnresolvedBatchKeysStmt.QueryxContext(ctx, sql.Named("L", limit))
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	type row struct {
		Number uint64 `db:"num"`
		Hash   string `db:"hash"`
	}

	var bks []types.BatchKey
	for rows.Next() {
		bk := row{}
		if err = rows.StructScan(&bk); err != nil {
			return nil, err
		}

		bks = append(bks, types.BatchKey{
			Number: bk.Number,
			Hash:   common.HexToHash(bk.Hash),
		})
	}

	return bks, nil
}

// DeleteUnresolvedBatchKeys deletes the unresolved batch keys from the database
func (db *msqlDB) DeleteUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	if len(bks) == 0 {
		return nil
	}

	const columnsAffected = 2

	args := make([]interface{}, len(bks)*columnsAffected)
	values := make([]string, len(bks))
	for i, bk := range bks {
		values[i] = fmt.Sprintf("(num = @N%d AND hash = @H%d)", i, i) //nolint:mnd
		args[i*columnsAffected] = sql.Named(fmt.Sprintf("N%d", i), bk.Number)
		args[i*columnsAffected+1] = sql.Named(fmt.Sprintf("H%d", i), bk.Hash.Hex())
	}

	query := fmt.Sprintf(`
		DELETE FROM data_node.unresolved_batches WHERE %s;
	`, strings.Join(values, " OR "))

	if _, err := db.msql.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to delete unresolved batches: %w", err)
	}

	return nil
}

// StoreOffChainData stores and array of key values in the Db
func (db *msqlDB) StoreOffChainData(ctx context.Context, ods []types.OffChainData) error {
	if len(ods) == 0 {
		return nil
	}

	query, args := buildOffchainDataInsertQuery(ods)

	if _, err := db.msql.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to store offchain data: %w", err)
	}

	return nil
}

// GetOffChainData returns the value identified by the key
func (db *msqlDB) GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error) {
	data := struct {
		Key      string `db:"key"`
		Value    string `db:"value"`
		BatchNum uint64 `db:"batch_num"`
	}{}

	if err := db.getOffChainDataStmt.QueryRowxContext(ctx, sql.Named("Key", key.Hex())).StructScan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrStateNotSynchronized
		}

		return nil, err
	}

	return &types.OffChainData{
		Key:      common.HexToHash(data.Key),
		Value:    common.FromHex(data.Value),
		BatchNum: data.BatchNum,
	}, nil
}

// ListOffChainData returns values identified by the given keys
func (db *msqlDB) ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	preparedKeys := make([]string, len(keys))
	for i, key := range keys {
		preparedKeys[i] = key.Hex()
	}

	query, args, err := sqlx.In(listOffchainDataSQL, preparedKeys)
	if err != nil {
		return nil, err
	}

	// sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend
	query = db.msql.Rebind(query)

	rows, err := db.msql.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	type row struct {
		Key      string `db:"key"`
		Value    string `db:"value"`
		BatchNum uint64 `db:"batch_num"`
	}

	list := make([]types.OffChainData, 0, len(keys))
	for rows.Next() {
		data := row{}
		if err = rows.StructScan(&data); err != nil {
			return nil, err
		}

		list = append(list, types.OffChainData{
			Key:      common.HexToHash(data.Key),
			Value:    common.FromHex(data.Value),
			BatchNum: data.BatchNum,
		})
	}

	return list, nil
}

// CountOffchainData returns the count of rows in the offchain_data table
func (db *msqlDB) CountOffchainData(ctx context.Context) (uint64, error) {
	var count uint64
	if err := db.countOffChainDataStmt.QueryRowContext(ctx).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

// DetectOffchainDataGaps returns the number of gaps in the offchain_data table
func (db *msqlDB) DetectOffchainDataGaps(ctx context.Context) (map[uint64]uint64, error) {
	rows, err := db.detectOffChainDataGapsStmt.QueryxContext(ctx)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	type row struct {
		CurrentBatchNum uint64 `db:"current_batch_num"`
		NextBatchNum    uint64 `db:"next_batch_num"`
	}

	gaps := make(map[uint64]uint64)
	for rows.Next() {
		var data row
		if err = rows.StructScan(&data); err != nil {
			return nil, err
		}

		gaps[data.CurrentBatchNum] = data.NextBatchNum
	}

	return gaps, nil
}

// buildBatchKeysInsertQuery builds the query to insert unresolved batch keys
func buildBatchKeysInsertQuery(bks []types.BatchKey) (string, []interface{}) {
	const columnsAffected = 2

	args := make([]interface{}, len(bks)*columnsAffected)
	values := make([]string, len(bks))
	for i, bk := range bks {
		values[i] = fmt.Sprintf("(@N%d, @H%d)", i, i) //nolint:mnd
		args[i*columnsAffected] = sql.Named(fmt.Sprintf("N%d", i), bk.Number)
		args[i*columnsAffected+1] = sql.Named(fmt.Sprintf("H%d", i), bk.Hash.Hex())
	}

	return fmt.Sprintf(`
		MERGE INTO data_node.unresolved_batches AS target
		USING (VALUES %s) AS source (num, hash)
		ON (target.num = source.num AND target.hash = source.hash)
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (num, hash) VALUES (source.num, source.hash);
`, strings.Join(values, ",")), args
}

// buildOffchainDataInsertQuery builds the query to insert offchain data
func buildOffchainDataInsertQuery(ods []types.OffChainData) (string, []interface{}) {
	const columnsAffected = 3

	// Remove duplicates from the given offchain data
	ods = types.RemoveDuplicateOffChainData(ods)

	args := make([]interface{}, len(ods)*columnsAffected)
	values := make([]string, len(ods))
	for i, od := range ods {
		values[i] = fmt.Sprintf("(@K%d, @H%d, @N%d)", i, i, i) //nolint:mnd
		args[i*columnsAffected] = sql.Named(fmt.Sprintf("K%d", i), od.Key.Hex())
		args[i*columnsAffected+1] = sql.Named(fmt.Sprintf("H%d", i), common.Bytes2Hex(od.Value))
		args[i*columnsAffected+2] = sql.Named(fmt.Sprintf("N%d", i), od.BatchNum)
	}

	return fmt.Sprintf(`
		MERGE INTO data_node.offchain_data AS target
		USING (VALUES %s) AS source ([key], [value], batch_num)
		ON target.[key] = source.[key]
		WHEN MATCHED THEN 
			UPDATE SET target.[value] = source.[value], target.batch_num = source.batch_num
		WHEN NOT MATCHED THEN 
			INSERT ([key], [value], batch_num)
			VALUES (source.[key], source.[value], source.batch_num);
	`, strings.Join(values, ",")), args
}
