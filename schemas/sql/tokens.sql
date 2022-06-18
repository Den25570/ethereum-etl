CREATE TABLE IF NOT EXISTS tokens (
    address varchar,
    symbol varchar,
    name varchar,
    decimals BIGINT,
    total_supply DECIMAL(38,0)
);
