from config.networkprovider import NetworkProvider 
from dataextractors.stablecoin import StablecoinProvider
from dataextractors.blocksprovider import BlocksProvider
from dataextractors.metacoin import MetacoinProvider
import datetime
import asyncio
import utils.xlsxexport
from dataextractors.protocolsdistribution import ProtocolProvider
import api.dynamodb as dynamodb
from decimal import Decimal
import utils.jsonprovider
from time import gmtime, strftime

async def blocks(start, end, interval):
    return await BlocksProvider.getBlocksByTime(start, end, interval)

# A section

def BtcToUsd(timestamps, values):
    rStart = datetime.datetime.fromtimestamp(timestamps[0]).strftime("%Y-%m-%d")
    rEnd = datetime.datetime.fromtimestamp(timestamps[len(timestamps) - 1]).strftime("%Y-%m-%d")
    btcPrice = {}
    if (rStart != rEnd):
        btcPrice = NetworkProvider.SendHttpRequest('BTC_PRICE', {'start': rStart, 'end': rEnd})['bpi']
    if rEnd == datetime.datetime.today().strftime('%Y-%m-%d'):
        btcPriceCurrent = NetworkProvider.SendHttpRequest('BTC_CURRENT_PRICE', {})['bpi']['USD']['rate_float']
        btcPrice[rEnd] = btcPriceCurrent
    lastBtcPrice = 0
    for i in range(len(timestamps)):
        data = datetime.datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
        if data in btcPrice:
            lastBtcPrice = btcPrice[data]
        values[i] = values[i] *  lastBtcPrice
    return values

async def musdTVL(timestamps, blocks, blocksPolygon):
    _, musdTVL = await StablecoinProvider.getMasset('USD', False, blocks, timestamps)
    _, musdTVLPolygon = await StablecoinProvider.getMasset('USD_POLYGON', False, blocksPolygon, timestamps, 'POL_MAINNET')
    return musdTVL, musdTVLPolygon

async def mbtcTVL(timestamps, blocks, inBTC = True):
    _, btcTVL =  await StablecoinProvider.getMasset('BTC', False, blocks, timestamps)
    if (not inBTC):
        btcTVL = BtcToUsd(timestamps, btcTVL)
    return btcTVL

async def gusdTVL(timestamps, blocks):
    _, gusdTVL = await StablecoinProvider.getMasset('GUSD', True, blocks, timestamps)
    return gusdTVL

async def busdTVL(timestamps, blocks):
    _, busdTVL =  await StablecoinProvider.getMasset('BUSD', True, blocks, timestamps)
    return busdTVL

async def tbtcTVL(timestamps, blocks, inBTC = True):
    _, tbtcTVL =  await StablecoinProvider.getMasset('TBTC', True, blocks, timestamps)
    if (not inBTC):
        tbtcTVL = BtcToUsd(timestamps, tbtcTVL)
    return tbtcTVL

async def hbtcTVL(timestamps, blocks, inBTC = True):
    _, hbtcTVL = await StablecoinProvider.getMasset('HBTC', True, blocks, timestamps)
    if (not inBTC):
       hbtcTVL = BtcToUsd(timestamps, hbtcTVL)
    return hbtcTVL

async def alusdTVL(timestamps, blocks):
    _, alusdTVL = await StablecoinProvider.getMasset('ALUSD', True, blocks, timestamps)
    return hbtcTVL

async def totalTVL(timestamps, blocks, blocksPolygon, additionalData):
    #running too many connections to subgraph results in unhandled exception, this code keeps 3 at the time
    getUsdTask = asyncio.create_task(musdTVL(timestamps, blocks, blocksPolygon))
    getGusdTask = asyncio.create_task(gusdTVL(timestamps, blocks))
    getBusdTask = asyncio.create_task(busdTVL(timestamps, blocks))
    getBtcTask = asyncio.create_task(mbtcTVL(timestamps, blocks))
    getTbtcTask = asyncio.create_task(tbtcTVL(timestamps, blocks))
    getHbtcTask = asyncio.create_task(hbtcTVL(timestamps, blocks))
    
    usdSupply, usdSupplyPolygon = await getUsdTask
    print(usdSupply, usdSupplyPolygon)
    gusdSupply = await getGusdTask
    busdSupply = await getBusdTask
    btcSupplyInBTC = await getBtcTask
    tbtcSupplyInBTC = await getTbtcTask
    hbtcSupplyInBTC = await getHbtcTask

    btcSupply = []
    tbtcSupply = []
    hbtcSupply = []
    
    totalTVL = []
    rStart = datetime.datetime.fromtimestamp(timestamps[0]).strftime("%Y-%m-%d")
    rEnd = datetime.datetime.fromtimestamp(timestamps[len(timestamps) - 1]).strftime("%Y-%m-%d")
    btcPrice = {}
    if (rStart != rEnd):
        btcPrice = NetworkProvider.SendHttpRequest('BTC_PRICE', {'start': rStart, 'end': rEnd})['bpi']
    if rEnd == datetime.datetime.today().strftime('%Y-%m-%d'):
        btcPriceCurrent = NetworkProvider.SendHttpRequest('BTC_CURRENT_PRICE', {})['bpi']['USD']['rate_float']
        btcPrice[rEnd] = btcPriceCurrent
    lastBtcPrice = btcPrice[datetime.datetime.fromtimestamp(timestamps[0]).strftime("%Y-%m-%d")]
    for i in range(len(timestamps)):
        data = datetime.datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
        if data in btcPrice:
            lastBtcPrice = btcPrice[data]
        btcSupply.append(btcSupplyInBTC[i] *  lastBtcPrice)
        tbtcSupply.append(tbtcSupplyInBTC[i] *  lastBtcPrice)
        hbtcSupply.append(hbtcSupplyInBTC[i] *  lastBtcPrice)

        sum = usdSupply[i] + btcSupply[i] + gusdSupply[i] + busdSupply[i] + tbtcSupply[i] + hbtcSupply[i]
        for j in range(len(additionalData)):
            sum += float(additionalData[j][i])
        totalTVL.append(sum)

    return totalTVL, usdSupply, btcSupply, gusdSupply, busdSupply, tbtcSupply, hbtcSupply, btcSupplyInBTC, tbtcSupplyInBTC, hbtcSupplyInBTC, usdSupplyPolygon

async def metaStack(timestamps, blocks):
    return await MetacoinProvider.getMtaStack(blocks, timestamps, "ETH_MAINNET")

async def metaCap(start, end):
    timestampsCaps, metaCaps = await MetacoinProvider.getMtaCap(start, end)
    return timestampsCaps, metaCaps

# B section

async def usdBassetShare(timestamps, blocks):
    #running too many connections to subgraph results in unhandled exception, this code keeps 3 at the time

    getSusdTask = asyncio.create_task(StablecoinProvider.getBasset('SUSD', blocks, timestamps))
    getDaiTask = asyncio.create_task(StablecoinProvider.getBasset('DAI', blocks, timestamps))
    getUsdcTask = asyncio.create_task(StablecoinProvider.getBasset('USDC', blocks, timestamps))
    susdTVL = await getSusdTask
    getUsdtTask = asyncio.create_task(StablecoinProvider.getBasset('USDT', blocks, timestamps))
    daiTVL = await getDaiTask
    usdcTVL = await getUsdcTask
    usdtTVL = await getUsdtTask
    
    percantages = StablecoinProvider.calculateBacketShare([susdTVL, daiTVL, usdcTVL, usdtTVL], True)

    return (susdTVL, percantages[0]), (daiTVL, percantages[1]), (usdcTVL, percantages[2]), (usdtTVL, percantages[3])

def usdProtocolDistributions(blocks):
    pp = ProtocolProvider(ProtocolProvider.InfuraProvider)
    pp.syncTransfers(['USD'])
    accountsUSD, balancesUSD = pp.getTokenBalances('USD', blocks, 50000)

    alusdSupply = [0 for i in range(len(blocks))]
    for i in range(len(accountsUSD)):
        if str(accountsUSD[i]) == str(NetworkProvider.ASSET_ADRESSES['ALUSD']):
            alusdSupply = balancesUSD[i]
            break

    print(alusdSupply)
    return accountsUSD, balancesUSD, alusdSupply

def mtaTotalAddresses(blocks):
    pp = ProtocolProvider(ProtocolProvider.InfuraProvider)
    pp.syncTransfers(['MTA'])
    accountsMTA, balancesMTA = pp.getTokenBalances('MTA', blocks, 0)
    mtaTotal = []

    for i in range(len(blocks)):
        blockTotal = 0
        for j in range(len(balancesMTA)):
            if balancesMTA[j][i] > 0:
                blockTotal = blockTotal + 1
        mtaTotal.append(blockTotal)
    return mtaTotal

# C section
async def btcBassetShare(timestamps, blocks):
    #running too many connections to subgraph results in unhandled exception, this code keeps 3 at the time

    getWbtcTask = asyncio.create_task(StablecoinProvider.getBasset('WBTC', blocks, timestamps))
    getRenbtcTask = asyncio.create_task(StablecoinProvider.getBasset('RENBTC', blocks, timestamps))
    getSbtcTask = asyncio.create_task(StablecoinProvider.getBasset('SBTC', blocks, timestamps))

    wbtcTVL = await getWbtcTask
    renbtcTVL = await getRenbtcTask
    sbtcTVL = await getSbtcTask
    
    percantages = StablecoinProvider.calculateBacketShare([wbtcTVL, renbtcTVL, sbtcTVL], True)

    return (wbtcTVL, percantages[0]), (renbtcTVL, percantages[1]), (sbtcTVL, percantages[2])

def btcProtocolDistributions(blocks):
    pp = ProtocolProvider(ProtocolProvider.InfuraProvider)
    pp.syncTransfers(['BTC'])
    accountsBTC, balancesBTC = pp.getTokenBalances('BTC', blocks, 1)
    return accountsBTC, balancesBTC

def getItems(data, labels):
    items = []
    for itemIndex in range(len(data[0])):
        item = {}
        for labelIndex in range(len(labels)):
            if len(data[labelIndex]) > 0:
                if (type(data[labelIndex][itemIndex]) is dict):
                    item[labels[labelIndex]] = data[labelIndex][itemIndex]
                elif labels[labelIndex] == 'date':
                    item[labels[labelIndex]] = str(data[labelIndex][itemIndex])
                else:
                    item[labels[labelIndex]] = Decimal(str(data[labelIndex][itemIndex]))
        items.append(item)
    return items

# D section
async def getVolumeMetrics(timestamps, blocks):
    getMusdVolumesTask = asyncio.create_task(StablecoinProvider.getMassetVolumeMetrics('USD', blocks, timestamps))
    getMbtcVolumesTask = asyncio.create_task(StablecoinProvider.getMassetVolumeMetrics('BTC', blocks, timestamps))
    volumeMetricsUsd = await getMusdVolumesTask
    volumeMetricsBtc = await getMbtcVolumesTask
    return volumeMetricsUsd, volumeMetricsBtc

def exportToXlsx(blocks, timestamps, 
        totalSupply, usdSupply, btcSupply ,gusdSupply, busdSupply, tbtcSupply, hbtcSupply,
        btcSupplyinbtc,
        stackValues, totalStakers,
        susdP, daidP ,usdcP, usdtP,
        susdTVL, daiTVL ,usdcTVL, usdtTVL,
        wbtcP, renbtcP ,sbtcP,
        wbtcTVL, renbtcTVL ,sbtcTVL, accountsUSD, balancesUSD, balancesBTC, accountsBTC):
    export = utils.xlsxexport.XlsxExporter('mstable.xlsx')

    export.exportToXlsxAsCols('mUsd protocol distribution', [timestamps] + balancesUSD, True, ['Dates'] + accountsUSD)
    export.exportToXlsxAsCols('mBtc protocol distribution', [timestamps] + balancesBTC, True, ['Dates'] + accountsBTC)

    export.exportToXlsxAsCols('all', 
    [blocks, timestamps, 
     totalSupply, usdSupply, btcSupply ,gusdSupply, busdSupply, tbtcSupply, hbtcSupply,
     btcSupplyinbtc,
     stackValues,
     susdP, daidP ,usdcP, usdtP,
     susdTVL, daiTVL ,usdcTVL, usdtTVL,
     wbtcP, renbtcP ,sbtcP,
     wbtcTVL, renbtcTVL ,sbtcTVL],  True,
    ['Block', 'Date', 
     'Total TVL$', 'USD TVL$', 'BTC TVL$', 'GUSD TVL$', 'BUSD TVL$', 'TBTC TVL$', 'HBTC TVL$',
     'BTC TVL',
     'mta stack', 'mta total addresses',
     'sUSD%', 'DAI%', 'USDC%', 'USDT%', 
     'sUSD$', 'DAI$', 'USDC$', 'USDT$', 
     'wBTC%', 'renBTC%', 'sBTC%', 
     'wBTC', 'renBTC', 'sBTC'])

    export.close()

async def main():

    db = dynamodb.DynamoDbConnection()
    interval = 60*60
    safetyInterval = 60*5
    currentDate = 0

    while currentDate < (int(datetime.datetime.now().timestamp() - safetyInterval * 2)):
        block = utils.jsonprovider.readJson(NetworkProvider.LOCAL_STORAGES['BLOCK']['BLOCK_CACHE'])
        if block == None:
            launchDate = NetworkProvider.ASSET_CREATION_TIMESTAMP['USD']
        else:
            items = db.getLatestTimestamp(block['date'], block['lastBlock'])
            if len(items['Items']) == 0:
                launchDate = NetworkProvider.ASSET_CREATION_TIMESTAMP['USD']
            else:
                launchDate = int(items['Items'][0]['timestamp']) + interval

        #launchDate = int(datetime.datetime.now().timestamp() - 86400)
        currentDate = min(launchDate + interval * 100, int(datetime.datetime.now().timestamp() - safetyInterval))

        #getting avaible blocks
        timestamps, blocks = await BlocksProvider.getBlocksByTime(launchDate, currentDate, interval)
        print(strftime("%H:%M:%S", gmtime()), "blocks ethernet:" + str(len(blocks)))
        timestampsPolygon, blocksPolygon = await BlocksProvider.getBlocksByTime(launchDate, currentDate, interval, 'POL_MAINNET')
        print(strftime("%H:%M:%S", gmtime()), "blocks polygon:" + str(len(blocksPolygon)))

        accountsUSD, balancesUSD, alusdSupply = usdProtocolDistributions(blocks)
        usdDistribution = getItems(balancesUSD, accountsUSD)
        print(strftime("%H:%M:%S", gmtime()), "musd distribution received")

        mtaTotal = mtaTotalAddresses(blocks)
        print(strftime("%H:%M:%S", gmtime()), "mtaTotal data received", mtaTotal)

        accountsBTC, balancesBTC = btcProtocolDistributions(blocks)
        btcDistribution = getItems(balancesBTC, accountsBTC)
        print(strftime("%H:%M:%S", gmtime()), "mbtc distribution received")

        volumeMetricsUsd, volumeMetricsBtc = await getVolumeMetrics(timestamps, blocks)
        print(strftime("%H:%M:%S", gmtime()), "volume data received")

        totalSupply, usdSupply, btcSupply, gusdSupply, busdSupply, tbtcSupply, hbtcSupply, btcSupplyinbtc, tbtcSupplyinbtc, hbtcSupplyinbtc, usdSupplyPolygon  = await totalTVL(timestamps, blocks, blocksPolygon, [alusdSupply])
        print(strftime("%H:%M:%S", gmtime()), "tvl received")

        stackValues = await metaStack(timestamps, blocks)
        print(strftime("%H:%M:%S", gmtime()), "mta data received")

        (susdTVL, susdP), (daiTVL, daidP), (usdcTVL, usdcP), (usdtTVL, usdtP) = await usdBassetShare(timestamps, blocks)
        print(strftime("%H:%M:%S", gmtime()), "musd basset data received")

        (wbtcTVL, wbtcP), (renbtcTVL, renbtcP), (sbtcTVL, sbtcP) = await btcBassetShare(timestamps, blocks)
        print(strftime("%H:%M:%S", gmtime()), "mbtc basset received")
        
        blocks = [int(blocks[f"t{ts}"][0]['number']) for ts in timestamps]
        dates = [datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d") for ts in timestamps]
        hours = [int(datetime.datetime.fromtimestamp(ts).strftime("%H")) for ts in timestamps]
        
        db.uploadBlocks(getItems(
            [dates, hours, timestamps, blocks, 
            totalSupply, usdSupply, btcSupply, gusdSupply, busdSupply, alusdSupply, 
            tbtcSupply, hbtcSupply, btcSupplyinbtc, tbtcSupplyinbtc, hbtcSupplyinbtc, 
            usdSupplyPolygon,
            stackValues, mtaTotal,
            susdP, daidP ,usdcP, usdtP,
            susdTVL, daiTVL ,usdcTVL, usdtTVL,
            wbtcP, renbtcP ,sbtcP,
            wbtcTVL, renbtcTVL ,sbtcTVL, usdDistribution, btcDistribution,
            
            volumeMetricsUsd['mint'], volumeMetricsUsd['swap'], volumeMetricsUsd['redemption'], volumeMetricsUsd['fees'],
            volumeMetricsUsd['deposit'][1], volumeMetricsUsd['deposit'][2], volumeMetricsUsd['withdraw'][1], volumeMetricsUsd['withdraw'][2],
            volumeMetricsUsd['apy'][1], volumeMetricsUsd['apy'][2], volumeMetricsUsd['saveUtilization'][1], volumeMetricsUsd['saveUtilization'][2],

            volumeMetricsBtc['mint'], volumeMetricsBtc['swap'], volumeMetricsBtc['redemption'], volumeMetricsBtc['fees'],
            volumeMetricsBtc['deposit'][1], volumeMetricsBtc['deposit'][2], volumeMetricsBtc['withdraw'][1], volumeMetricsBtc['withdraw'][2],
            volumeMetricsBtc['apy'][1], volumeMetricsBtc['apy'][2], volumeMetricsBtc['saveUtilization'][1], volumeMetricsBtc['saveUtilization'][2],
            ],
            ['date', 'hour', 'timestamp', 'blockNumber', 
            'totalSupply', 'usdSupply', 'btcSupply', 'gusdSupply', 'busdSupply', 'alusdSupply',
            'tbtcSupply', 'hbtcSupply', 'btcSupplyinbtc', 'tbtcSupplyinbtc', 'hbtcSupplyinbtc', 
            'usdSupplyPolygon',
            'mtaStack', 'mtaTotal',
            'susdPercent', 'daiPercent', 'usdcPercent', 'usdtPercent', 
            'susdValue', 'daiValue', 'usdcValue', 'usdtValue', 
            'wbtcPercent', 'renbtcPercent', 'sbtcPercent', 
            'wbtcValue', 'renbtcValue', 'sbtcValue', 'musdDistribution', 'mbtcDistribution',

            'usdCumulativeMinted', 'usdCumulativeSwapped', 'usdCumulativeRedeemed', 'usdCumulativeFeesPaid', 
            'usdCumulativeDepositedV1', 'usdCumulativeDepositedV2', 'usdCumulativeWithdrawnV1', 'usdCumulativeWithdrawnV2', 
            'usdDailyApyV1', 'usdDailyApyV2', 'usdUtilisationRateV1', 'usdUtilisationRateV2', 
            'btcCumulativeMinted', 'btcCumulativeSwapped', 'btcCumulativeRedeemed', 'btcCumulativeFeesPaid', 
            'btcCumulativeDepositedV1', 'btcCumulativeDepositedV2', 'btcCumulativeWithdrawnV1', 'btcCumulativeWithdrawnV2', 
            'btcDailyApyV1', 'btcDailyApyV2', 'btcUtilisationRateV1', 'btcUtilisationRateV2', 
            ]
        ))
        print(f"data uploaded from {launchDate} to {currentDate}")
        utils.jsonprovider.writeJson(NetworkProvider.LOCAL_STORAGES['BLOCK']['BLOCK_CACHE'], {"date": dates[len(dates)-1],"lastBlock" : blocks[len(blocks) - 1]})
    
asyncio.run(main())
