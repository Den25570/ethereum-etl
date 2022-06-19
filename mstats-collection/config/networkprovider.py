from gql.transport.aiohttp import AIOHTTPTransport
import requests


class NetworkProvider:

    GQL_END_POINTS = {
        'ETH_MAINNET': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-staging',
            'feeders': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-feeder-pools',
            'blocks': 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks',
            'governance': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-governance-staging'
        },

        'POL_MAINNET': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-polygon',
            'blocks': 'https://api.thegraph.com/subgraphs/name/ord786/matic-blocks',
        },

        'ETH_ROPSTEN': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-ropsten',
            'feeders': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-feeders-ropsten',
            'blocks': 'https://api.thegraph.com/subgraphs/name/blocklytics/ropsten-blocks',
            'ecosystem': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-ecosystem',
            # mainnet
            'governance': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-governance-staging'
        },

        'ETH_GOERLI': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-goerli',
            'feeders': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-feeders-goerli',
            'blocks': 'https://api.thegraph.com/subgraphs/name/blocklytics/goerli-blocks',
            # mainnet
            'governance': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-governance-staging'
        },

        'MATIC_MAINNET': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-polygon',
            'feeders': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-feeder-pools-polygon',
            'blocks': 'https://api.thegraph.com/subgraphs/name/elkfinance/matic-blocks',
            # mainnet
            'ecosystem': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-ecosystem',
            'governance': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-governance-staging'
        },

        'MATIC_MUMBAI': {
            'protocol': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol-polygon-mumbai',
            'feeders': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-feeder-pools-mumbai',
            # mainnet
            'blocks': 'https://api.thegraph.com/subgraphs/name/elkfinance/matic-blocks',
            'governance': 'https://api.thegraph.com/subgraphs/name/mstable/mstable-governance-staging'
        }
    }

    ASSET_ADRESSES = {
        # mAssets
        'USD': '0xe2f2a5c287993345a840db3b0845fbc70f5935a5',
        'USD_POLYGON': '0xe840b73e5287865eec17d250bfb1536704b43b21',
        'BTC': '0x945facb997494cc2570096c74b5f66a3507330a1',
        # Pool usd assets
        'GUSD': '0x4fb30c5a3ac8e85bc32785518633303c4590752d',
        'BUSD': '0xfe842e95f8911dcc21c943a1daa4bd641a1381c6',
        'ALUSD': '0x4eaa01974b6594c0ee62ffd7fee56cf11e6af936',
        # Pool btc assets
        'TBTC': '0xb61a6f928b3f069a68469ddb670f20eeeb4921e0',
        'HBTC': '0x48c59199da51b7e30ea200a74ea07974e62c4ba7',
        # Usd stablecoins
        'SUSD': '0x57ab1ec28d129707052df4df418d58a2d46d5f51',
        'DAI': '0x6b175474e89094c44da98b954eedeac495271d0f',
        'USDC': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        'USDT': '0xdac17f958d2ee523a2206206994597c13d831ec7',
        # btc stablecoins
        'WBTC': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
        'RENBTC': '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
        'SBTC': '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
        #Governance
        'MTA': '0xa3bed4e1c75d00fa6f4e5e6922db7261b5e9acd2'
    }

    ASSET_CREATION_BLOCK = {
        'USD': 10148032,
        'BTC': 11840521,
        'MTA': 10148032,

    }

    ASSET_CREATION_TIMESTAMP = {
        'USD': 1590462000,
        'BTC': 1613012400,
        'MTA': 1590462000,
        'MTA_STAKE_V2': 1633791600,
    }

    HTTP_ENDPOINTS = {
        'BTC_PRICE': 'http://api.coindesk.com/v1/bpi/historical/close.json',
        'BTC_CURRENT_PRICE': 'https://api.coindesk.com/v1/bpi/currentprice/CNY.json',
        'MTA_CAP': 'https://api.coingecko.com/api/v3/coins/meta/market_chart/range'
    }

    # https://mainnet.infura.io/v3/8a0bf5dde9ef4d48953791ffd0741478

    # ethereumetl export_token_transfers --start-block 10932900 --end-block 12500000 --provider-uri https://mainnet.infura.io/v3/8a0bf5dde9ef4d48953791ffd0741478 --output token_transfers.csv
    # docker run -v $HOME/output:/ethereum-etl/output ethereum-etl:latest export_token_transfers  -s 10932900 -e 12500000 -b 100000 -p https://mainnet.infura.io/v3/8a0bf5dde9ef4d48953791ffd0741478
    # docker run -v $HOME/output:/ethereum-etl/output ethereum-etl:latest export_token_transfers  -s 10932900 -e 12500000 -b 100000 -p https://mainnet.infura.io/v3/8a0bf5dde9ef4d48953791ffd0741478
    # ethereumetl export_blocks_and_transactions --start-block 10932900 --end-block 12500000 --provider-uri https://mainnet.infura.io/v3/8a0bf5dde9ef4d48953791ffd0741478 --blocks-output blocks.csv --transactions-output transactions.csv

    API_KEYS = {
        'ANYBLOCK': 'ab3466d7-3e4d-4fa9-a8c2-c10f6076e3bc',
        'COINMARKET': 'd4dc887e-1362-4f4b-9b9b-323c8bb1ef3f',
        'ETHERSCAN': 'TA9UVVWB9MI4TG8K4FAMP8382PMW9DAW8S',
        'INFURA': '4432125dd1b64d47b4aeb095cc1821d0'
    }

    LOCAL_STORAGES = {
        'USD': {
            'TRANSFERS_TEMP': 'storage/usd_temp_transfers.txt',
            'TRANSFERS': 'storage/usd_transfers.txt',
            'TRANSFERS_CACHE': 'storage/usd_transfers_cache.json'
        },
        'BTC': {
            'TRANSFERS_TEMP': 'storage/btc_temp_transfers.txt',
            'TRANSFERS': 'storage/btc_transfers.txt',
            'TRANSFERS_CACHE': 'storage/btc_transfers_cache.json'
        },
        'MTA': {
            'TRANSFERS_TEMP': 'storage/mta_temp_transfers.txt',
            'TRANSFERS': 'storage/mta_transfers.txt',
            'TRANSFERS_CACHE': 'storage/mta_transfers_cache.json'
        },
        'BLOCK': {
            'BLOCK_CACHE': 'storage/block_cache.json'
        }
    }

    SQL_ENDPOINTS = {
        # Server - sql.anyblock.net
        'ANYBLOCK': {
            'HOST': 'sql.anyblock.net',
            'PORT': '5432',
            'USERNAME': '17247f3c-3ba7-4735-8ef0-bf3c92bf924f',
            'PASSWORD': 'c8382e27-84e3-431b-99ff-2f35b593bd81',
            'DATABASES': {
                'ETHEREUM_ETHEREUM_MAINNET': 'ethereum_ethereum_mainnet',
                'ETHEREUM_ETHEREUM_ROPSTEN': 'ethereum_ethereum_ropsten',
                'ETHEREUM_ETHEREUM_GOERLI': 'ethereum_ethereum_goerli',
                'ETHEREUM_EWF_EWC': 'ethereum_ewf_ewc',
                'ETHEREUM_POA_CORE': 'ethereum_poa_core',
                'ETHEREUM_POA_SOKOL': 'ethereum_poa_sokol',
                'ETHEREUM_POA_XDAI': 'ethereum_poa_xdai',
                'ETHEREUM_TRUSTLINES_LAIKA': 'ethereum_trustlines_laika',
                'ETHEREUM_TRUSTLINES_TLBC': 'ethereum_trustlines_tlbc',
                'ETHEREUM_QUORUM_EXPORO': 'ethereum_quorum_exporo',
                'ETHEREUM_QUORUM_GAMESCHAIN': 'ethereum_quorum_gameschain',
                'ETHEREUM_QUORUM_LCLITE': 'ethereum_quorum_lclite',
            }
        },
        'LOCAL': {
            'HOST': '127.0.0.1',
            'PORT': '5433',
            'USERNAME': 'postgres',
            'PASSWORD': 'postgres',
            'DATABASES': {
                'TXS': 'txs'
            }
        }
    }

    @staticmethod
    def GetTransport(protocol, api):
        url = NetworkProvider.GQL_END_POINTS[protocol][api]
        return AIOHTTPTransport(url=url)

    @staticmethod
    def SendHttpRequest(endpoint, params):
        URL = NetworkProvider.HTTP_ENDPOINTS[endpoint]
        return requests.get(url=URL, params=params).json()
