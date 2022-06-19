from gql import gql, Client
import datetime
from config.networkprovider import NetworkProvider
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.transport import Transport
import numpy as np
from string import Template
import asyncio

class MetacoinProvider:

    blockTemplate = Template('t$ts: incentivisedVotingLockups(block: {number: $number}) { ...IncentivisedVotingLockupDetails }')

    @staticmethod
    async def getMtaStack(blocks, timestamps, address):
        # prepare data
        timestampBlocks = [[]]
        for i in range(len(timestamps)):
            if (i + 1) % 90 == 0:
                timestampBlocks.append([])
            timestampBlocks[len(timestampBlocks) - 1].append(timestamps[i])

        # generate query
        stackValues = []

        for tsBlock in timestampBlocks:
            query = gql("""
            query IncentivisedVotingLockups
                {
                    """+ '\n'.join([MetacoinProvider.blockTemplate.substitute(ts = ts, number = int(blocks[f't{ts}'][0]['number'])) for ts in tsBlock]) +"""
                }

                fragment IncentivisedVotingLockupDetails on IncentivisedVotingLockup {
                    address: id
                    totalValue
                }
            """)

            # retrieve data
            transportProtocols = NetworkProvider.GetTransport(address, 'governance')
            clientProtocols = Client(transport=transportProtocols, fetch_schema_from_transport=True)
            data = await clientProtocols.execute_async(query)

            stackValueAtBlock = 0 
            for ts in tsBlock:
                if data[f't{ts}'] != None and len(data[f't{ts}']) != 0:
                    stackValueAtBlock = float(data[f't{ts}'][0]['totalValue'])
                stackValues.append(stackValueAtBlock)

        return stackValues

    @staticmethod
    async def getMtaCap(start, end):

        metaCap = NetworkProvider.SendHttpRequest('MTA_CAP', 
        {
            'vs_currency': 'usd',
            'from': start, 
            'to': end
        })

        timestampsCaps = []
        metaCaps = []
        for cap in metaCap['market_caps']:
            timestampsCaps.append(cap[0])
            metaCaps.append(cap[1])

        return (timestampsCaps, metaCaps)