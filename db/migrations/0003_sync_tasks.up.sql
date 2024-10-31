BEGIN
    CREATE TABLE data_node.sync_tasks
    (
        task        VARCHAR(255) PRIMARY KEY,
        block       BIGINT NOT NULL,
        processed   DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
    );
    INSERT INTO data_node.sync_tasks (task, block) VALUES ('L1', (SELECT MAX(block) FROM data_node.sync_info));
    DROP TABLE IF EXISTS data_node.sync_info;
END