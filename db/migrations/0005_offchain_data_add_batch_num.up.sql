BEGIN
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('data_node.offchain_data') AND name = 'batch_num')
        BEGIN
            ALTER TABLE data_node.offchain_data
                ADD batch_num BIGINT NOT NULL DEFAULT 0;
        END
    CREATE INDEX idx_batch_num ON data_node.offchain_data(batch_num);
    UPDATE data_node.sync_tasks SET block = 0 WHERE task = 'L1';
END