BEGIN
    CREATE TABLE data_node.sync_info
    (
        block       BIGINT PRIMARY KEY,
        processed   DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
    );
    INSERT INTO data_node.sync_info (block) VALUES (0);
END
