IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('data_node.offchain_data') AND name = 'batch_num')
    BEGIN
        ALTER TABLE data_node.offchain_data DROP COLUMN batch_num;
    END