IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data_node')
    EXEC('CREATE SCHEMA data_node')

CREATE TABLE data_node.offchain_data
(
    [key] VARCHAR(255) PRIMARY KEY,
    [value] VARCHAR(MAX)
);