CREATE TABLE IF NOT EXISTS receipts (
    transaction_hash varchar,
    transaction_index BIGINT,
    block_hash varchar,
    block_number BIGINT,
    cumulative_gas_used BIGINT,
    gas_used BIGINT,
    contract_address varchar,
    root varchar,
    status BIGINT
);
