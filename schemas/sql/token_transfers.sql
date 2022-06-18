CREATE TABLE IF NOT EXISTS token_transfers (
    token_address varchar,
    from_address varchar,
    to_address varchar,
    value DECIMAL(38,0),
    transaction_hash varchar,
    log_index BIGINT,
    block_number BIGINT
);