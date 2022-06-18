CREATE TABLE IF NOT EXISTS transactions (
    hash varchar,
    nonce BIGINT,
    block_hash varchar,
    block_number BIGINT,
    transaction_index BIGINT,
    from_address varchar,
    to_address varchar,
    value DECIMAL(38,0),
    gas BIGINT,
    gas_price BIGINT,
    input STRING
);