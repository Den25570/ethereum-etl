CREATE TABLE IF NOT EXISTS contracts (
    address varchar,
    bytecode varchar,
    function_sighashes varchar,
    is_erc20 BOOLEAN,
    is_erc721 BOOLEAN,
    is_erc1155 BOOLEAN
);
