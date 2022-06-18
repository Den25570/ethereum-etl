CREATE TABLE IF NOT EXISTS logs (
    log_index BIGINT,
    transaction_hash varchar,
    transaction_index BIGINT,
    block_hash varchar,
    block_number BIGINT,
    address varchar,
    data varchar,
    topics STRING
);
