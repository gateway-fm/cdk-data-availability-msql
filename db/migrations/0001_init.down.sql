BEGIN
    DROP TABLE IF EXISTS data_node.offchain_data;
    IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'data_node')
        BEGIN
            DROP SCHEMA data_node;
        END
END