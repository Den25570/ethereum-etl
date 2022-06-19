from gql import gql, Client
from config.networkprovider import NetworkProvider
from string import Template

class BlocksProvider:
    # templates
    BlockTemplate = Template("""t$ts: blocks(first: 1, orderBy: timestamp, orderDirection: asc, where: {timestamp_gt: $ts}) 
        { number }
        """)

    @staticmethod   
    async def getLatestBlock(network = "ETH_MAINNET"):
        query = gql("""
                {
                    blocks(first: 1, skip: 0, orderBy: number, orderDirection: desc, where: {number_gt: 9300000}) {
                        number
                        timestamp
                    }
                }
                """
            )
        transportBlocks = NetworkProvider.GetTransport(network, 'blocks')
        clientBlocks =  Client(transport=transportBlocks, fetch_schema_from_transport=True)
        result = await clientBlocks.execute_async(query)
        return (result['blocks'][0])


    @staticmethod    
    async def getBlocksByTime(launchDate, currentDate, interval, network = "ETH_MAINNET"):
        timestamps = [ts for ts in range(launchDate, currentDate, interval)]
        blocks = [BlocksProvider.BlockTemplate.substitute(ts=ts, tsInt=ts+interval) for ts in timestamps]

        query = gql("""
                query BlockTimestamps {
                    """+'\n'.join(blocks)+"""
                }
                """
            )

        transportBlocks = NetworkProvider.GetTransport(network, 'blocks')
        clientBlocks =  Client(transport=transportBlocks, fetch_schema_from_transport=True)
        result = await clientBlocks.execute_async(query)
        return (timestamps, result)
