from flask import Flask
from flask import request
import mstable

application = Flask(__name__)

# A section
@application.route('/total-tvl')
async def totalTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    totalTVL = await mstable.totalTVL(timestamps, blocks)
    return {'totalTVL': totalTVL}

@application.route('/musd-tvl')
async def musdTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    musdTVL = await mstable.musdTVL(timestamps, blocks)
    return {'musdTVL': musdTVL}

@application.route('/mbtc-tvl')
async def mbtcTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))
    inBTC = bool(request.args.get('inBTC'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    mbtcTVL = await mstable.mbtcTVL(timestamps, blocks, inBTC=inBTC)
    return {'mbtcTVL': mbtcTVL}

@application.route('/gusd-tvl')
async def gusdTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    gusdTVL = await mstable.gusdTVL(timestamps, blocks)
    return {'gusdTVL': gusdTVL}

@application.route('/busd-tvl')
async def busdTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    busdTVL = await mstable.busdTVL(timestamps, blocks)
    return {'busdTVL': busdTVL}

@application.route('/tbtc-tvl')
async def tbtcTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))
    inBTC = bool(request.args.get('inBTC'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    tbtcTVL = await mstable.tbtcTVL(timestamps, blocks, inBTC=inBTC)
    return {'tbtcTVL': tbtcTVL}

@application.route('/hbtc-tvl')
async def hbtcTVL():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))
    inBTC = bool(request.args.get('inBTC'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    hbtcTVL = await mstable.hbtcTVL(timestamps, blocks, inBTC=inBTC)
    return {'hbtcTVL': hbtcTVL}

@application.route('/mta-cap')
async def mtaCap():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestampsCaps, metaCaps = await mstable.metaCap(start, end)
    return {'mtaCap': metaCaps, 'timestamps': timestampsCaps}

@application.route('/mta-info')
async def mtaInfo():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    stackValues, totalStakers = await mstable.metaStack(timestamps, blocks)
    return {'stackValues': stackValues, 'totalStakers': totalStakers}

@application.route('/mta-staked')
async def mtaStacked():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    stackValues, _ = await mstable.metaStack(timestamps, blocks)
    return {'stackValues': stackValues}

@application.route('/mta-addresses')
async def mtaAddresses():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, totalStakers = await mstable.metaStack(timestamps, blocks)
    return {'totalStakers': totalStakers}

# B section

@application.route('/usd-share')
async def usdShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    susdTVL, daiTVL, usdcTVL, usdtTVL = await mstable.usdBassetShare(timestamps, blocks)
    return {'susdTVL': susdTVL, 'daiTVL': daiTVL, 'usdcTVL': usdcTVL, 'usdtTVL': usdtTVL}

@application.route('/dai-share')
async def daiShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    susdTVL, _, _, _ = await mstable.usdBassetShare(timestamps, blocks)
    return {'susdTVL': susdTVL}

@application.route('/susd-share')
async def susdShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, daiTVL, _, _ = await mstable.usdBassetShare(timestamps, blocks)
    return {'daiTVL': daiTVL}

@application.route('/usdc-share')
async def usdcShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, _, usdcTVL, usdt_TVL = await mstable.usdBassetShare(timestamps, blocks)
    return {'usdcTVL': usdcTVL}

@application.route('/usdt-share')
async def usdtShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, _, _, usdtTVL = await mstable.usdBassetShare(timestamps, blocks)
    return {'usdtTVL': usdtTVL}

@application.route('/musd-distribution')
async def musdDistribution():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    _, blocks = await mstable.blocks(start, end, interval)
    accountsUSD, balancesUSD = mstable.usdProtocolDistributions(blocks)
    print(accountsUSD, balancesUSD) 
    return {'accountsUSD': accountsUSD, 'balancesUSD': balancesUSD,}

# C section

@application.route('/btc-share')
async def btcShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    wbtcTVL, renbtcTVL, sbtcTVL = await mstable.btcBassetShare(timestamps, blocks)
    return {'wbtcTVL': wbtcTVL, 'renbtcTVL': renbtcTVL, 'sbtcTVL': sbtcTVL}

@application.route('/wbtc-share')
async def wbtcShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    wbtcTVL, _, _ = await mstable.btcBassetShare(timestamps, blocks)
    return {'wbtcTVL': wbtcTVL}

@application.route('/renbtc-share')
async def renbtcShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, renbtcTVL, _ = await mstable.btcBassetShare(timestamps, blocks)
    return {'renbtcTVL': renbtcTVL}

@application.route('/sbtc-share')
async def sbtcShare():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    timestamps, blocks = await mstable.blocks(start, end, interval)
    _, _, sbtcTVL = await mstable.btcBassetShare(timestamps, blocks)
    return {'sbtcTVL': sbtcTVL}

@application.route('/mbtc-distribution')
async def mbtcDistribution():
    start = int(request.args.get('start'))
    end = int(request.args.get('end'))
    interval = int(request.args.get('interval'))

    _, blocks = await mstable.blocks(start, end, interval)
    accountsBTC, balancesBTC = mstable.btcProtocolDistributions(blocks)

    return {'accountsBTC': accountsBTC, 'balancesBTC': balancesBTC}

@application.route('/update')
async def update():
    await mstable.main()

if __name__ == "__main__":
    application.debug = True
    application.run()