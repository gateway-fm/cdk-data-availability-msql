CREATE TABLE data_node.unresolved_batches
(
    num         BIGINT NOT NULL,
    hash        VARCHAR(255) NOT NULL,
    created_at  DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    PRIMARY KEY (num, hash)
);