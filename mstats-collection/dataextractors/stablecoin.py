import math
from gql import gql, Client
from config.networkprovider import NetworkProvider
from string import Template

class StablecoinProvider:
    # templates
    MassetTvlTemplate = Template('t$timestamp: masset(id: "$poolAddress", block: { number: $number }) { ...AggregateMetricsFields }')
    PoolTvlTemplate = Template('t$timestamp: feederPool(id: "$poolAddress", block: { number: $number }) { ...AggregateMetricsFields }')
    BassetTvlTemplate = Template('t$timestamp: basset(id: "$poolAddress", block: { number: $number }) { ...AggregateMetricsFields }')
    MassetVolumeMetrics = Template('t$timestamp: masset(id: "$poolAddress", block: { number: $number }) { ...VolumeMetricsFields }')

    @staticmethod
    def splitTimestampsIntoBlocks(timestamps, blocksNumber):
        timestampBlocks = [[]]

        for i in range(len(timestamps)):
            if (i + 1) % blocksNumber == 0:
                timestampBlocks.append([])
            timestampBlocks[len(timestampBlocks) - 1].append(timestamps[i])
        return timestampBlocks

    @staticmethod
    def calculateBacketShare(coins, format = False):
        percentages = [[] for i in range(len(coins))]
        for i in range(len(coins[0])):
            totalValue = 0 
            for coinIndex in range(len(coins)):
                totalValue += coins[coinIndex][i]
            for coinIndex in range(len(coins)):
                if not format:
                    percentages[coinIndex].append((coins[coinIndex][i] / totalValue) if totalValue != 0 else 0)
                else:
                    percentages[coinIndex].append(math.floor((coins[coinIndex][i] / totalValue) * 10000) / 100 if totalValue != 0 else 0)
        return percentages

    @staticmethod
    async def getBasset(address, blocks, timestamps, network = 'ETH_MAINNET'):
        poolAddress = NetworkProvider.ASSET_ADRESSES[address]
        timestampBlocks = StablecoinProvider.splitTimestampsIntoBlocks(timestamps, 90)
        supply = []
        for tsBlock in timestampBlocks:
            query = gql("""
            fragment AggregateMetricsFields on Basset {
                vaultBalance {
                    simple
                }
            }         
            query AggregateMetrics { """ +
                '\n'.join([StablecoinProvider.BassetTvlTemplate.substitute(timestamp=ts, poolAddress=poolAddress, number=int(blocks[f't{ts}'][0]['number'])) for ts in tsBlock]) + """
            }
            """)
            # retrieve data
            transportProtocols = NetworkProvider.GetTransport(network, 'protocol')          
            clientProtocols = Client(transport=transportProtocols, fetch_schema_from_transport=True)
            data = await clientProtocols.execute_async(query)

            supplyValue = 0 
            for ts in tsBlock:
                if data[f't{ts}'] != None:
                    supplyValue = float(data[f't{ts}']['vaultBalance']['simple'])
                supply.append(supplyValue)
        return supply

    @staticmethod
    async def getMasset(address, isFeederPool, blocks, timestamps, network = 'ETH_MAINNET'):

        # prepare data
        poolAddress = NetworkProvider.ASSET_ADRESSES[address]
        timestampBlocks = StablecoinProvider.splitTimestampsIntoBlocks(timestamps, 90)

        # generate query
        savings = []
        supply = []
        for tsBlock in timestampBlocks:
            if not isFeederPool:
                query = gql("""
                fragment AggregateMetricsFields on Masset {
                    totalSupply { simple }
                    savingsContracts(orderBy: version, orderDirection: asc) {
                        version
                        latestExchangeRate { rate }
                        totalSavings { simple }
                    }
                }
                
                query AggregateMetrics {
                    """ +
                    '\n'.join([StablecoinProvider.MassetTvlTemplate.substitute(timestamp=ts, poolAddress=poolAddress, number=int(blocks[f't{ts}'][0]['number'])) for ts in tsBlock]) + """
                }
                """)
            else:
                query = gql("""
                fragment AggregateMetricsFields on FeederPool {
                    totalSupply { simple }
                }
                
                query AggregateMetrics { """ + 
                    '\n'.join([StablecoinProvider.PoolTvlTemplate.substitute(timestamp=ts, poolAddress=poolAddress, number=int(blocks[f't{ts}'][0]['number'])) for ts in tsBlock]) + """
                }
                """)

            # retrieve data
            if not isFeederPool:
                transportProtocols = NetworkProvider.GetTransport(network, 'protocol')
            else:
                transportProtocols = NetworkProvider.GetTransport(network, 'feeders')
            
            clientProtocols = Client(transport=transportProtocols, fetch_schema_from_transport=True)
            data = await clientProtocols.execute_async(query)

            v2ContarctValue = supplyValue = 0
            for ts in tsBlock:
                if data[f't{ts}'] != None:
                    if not isFeederPool and len(data[f't{ts}']['savingsContracts']) == 2:
                        v2ContarctValue = float(data[f't{ts}']['savingsContracts'][1]['totalSavings']['simple']) / 10
                    supplyValue = float(data[f't{ts}']['totalSupply']['simple'])

                savings.append(v2ContarctValue)
                supply.append(supplyValue)

        return (savings, supply)

    
    @staticmethod
    async def getMassetVolumeMetrics(address, blocks, timestamps, network = 'ETH_MAINNET'):
        poolAddress = NetworkProvider.ASSET_ADRESSES[address]
        timestampBlocks = StablecoinProvider.splitTimestampsIntoBlocks(timestamps, 30)

        volumeMetrics = {
            'mint': [],
            'swap': [],
            'redemption': [],
            'fees': [],
            'deposit': {1 : [], 2 : []},
            'withdraw': {1 : [], 2 : []},
            'apy': {1 : [], 2 : []},
            'saveUtilization': {1 : [], 2 : []},
        }
        
        for tsBlock in timestampBlocks:
            query = gql("""
            fragment DailySaveAPY on SavingsContract {
                dailyAPY
                utilisationRate { simple }
                latestExchangeRate { rate }
            }  
            fragment VolumeMetricsFields on Masset {
                cumulativeMinted { simple }
                cumulativeSwapped { simple }
                cumulativeRedeemed { simple }
                cumulativeRedeemedMasset { simple }
                cumulativeFeesPaid { simple }
                savingsContracts {
                    version
                    cumulativeDeposited { simple }
                    cumulativeWithdrawn { simple }
                    ...DailySaveAPY
                }
            }         
            query AggregateMetrics {
                """ + '\n'.join([StablecoinProvider.MassetVolumeMetrics.substitute(timestamp=ts, poolAddress=poolAddress, number=int(blocks[f't{ts}'][0]['number'])) for ts in tsBlock]) + """
            }
            """)

            # retrieve data
            transportProtocols = NetworkProvider.GetTransport(network, 'protocol')        
            clientProtocols = Client(transport=transportProtocols, fetch_schema_from_transport=True)
            data = await clientProtocols.execute_async(query)

            mintValue = swapValue = redemptionValue = feesValue = 0
            depositValue = {1 : 0, 2 : 0}
            withdrawValue = {1 : 0, 2 : 0}
            apyValue = {1 : 0, 2 : 0}
            saveUtilizationValue = {1 : 0, 2 : 0}
            for ts in tsBlock:
                if data[f't{ts}'] != None:
                    mintValue = float(data[f't{ts}']['cumulativeMinted']['simple'])
                    swapValue = float(data[f't{ts}']['cumulativeSwapped']['simple'])
                    redemptionValue = float(data[f't{ts}']['cumulativeRedeemed']['simple'])
                    feesValue = float(data[f't{ts}']['cumulativeFeesPaid']['simple'])
                    
                    for savingsContract in data[f't{ts}']['savingsContracts']:
                        depositValue[savingsContract['version']] = float(savingsContract['cumulativeDeposited']['simple'])
                        withdrawValue[savingsContract['version']] = float(savingsContract['cumulativeWithdrawn']['simple'])
                        apyValue[savingsContract['version']] = float(savingsContract['dailyAPY'])
                        if savingsContract['utilisationRate'] != None and savingsContract['latestExchangeRate'] != None:
                            saveUtilizationValue[savingsContract['version']] = float(savingsContract['utilisationRate']['simple']) * float(savingsContract['latestExchangeRate']['rate'])

                volumeMetrics['mint'].append(mintValue)           
                volumeMetrics['swap'].append(swapValue)
                volumeMetrics['redemption'].append(redemptionValue)
                volumeMetrics['fees'].append(feesValue)

                volumeMetrics['deposit'][1].append(depositValue[1])
                volumeMetrics['withdraw'][1].append(withdrawValue[1])
                volumeMetrics['deposit'][2].append(depositValue[2])
                volumeMetrics['withdraw'][2].append(withdrawValue[2])

                volumeMetrics['apy'][1].append(apyValue[1])
                volumeMetrics['saveUtilization'][1].append(saveUtilizationValue[1])
                volumeMetrics['apy'][2].append(apyValue[2])
                volumeMetrics['saveUtilization'][2].append(saveUtilizationValue[2])

        print('apy2', volumeMetrics['apy'][2])

        return volumeMetrics
