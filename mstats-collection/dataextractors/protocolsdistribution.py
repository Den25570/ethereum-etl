from eth_typing.evm import BlockNumber
from config.networkprovider import NetworkProvider 
import numpy as np
from web3 import Web3
import utils.csvreader
import math
import utils.jsonprovider
import os
import copy
 
class ProtocolProvider:
    InfuraProvider = 1
    LocalProvider = 2
    
    provider: str
    w3 = Web3()
    
    def __init__(self, provider):
        if provider == ProtocolProvider.InfuraProvider:
            projectId = NetworkProvider.API_KEYS['INFURA']
            self.provider = f'https://mainnet.infura.io/v3/{projectId}'
            self.w3 = Web3(Web3.HTTPProvider(self.provider))
        elif provider == ProtocolProvider.LocalProvider:
            self.provider = '\\.\pipe\geth.ipc'
            self.w3 = Web3(Web3.IPCProvider(self.provider))

    def syncTransfers(self, addresses):
      latestBlock = self.w3.eth.getBlock('latest')['number'] - 5 # ignore the last 5 blocks to avoid unwanted collisions

      for address in addresses:
        cache = utils.jsonprovider.readJson(NetworkProvider.LOCAL_STORAGES[address]['TRANSFERS_CACHE'])
        if cache != None:
          latestSyncedBlock = int(cache['lastBlock'])
        else:
          latestSyncedBlock = NetworkProvider.ASSET_CREATION_BLOCK[address]
        
        if latestSyncedBlock < latestBlock:
          outputFile = NetworkProvider.LOCAL_STORAGES[address]['TRANSFERS_TEMP']
          tokenAddress = self.w3.toChecksumAddress(NetworkProvider.ASSET_ADRESSES[address])
          
          cmd = f"ethereumetl export_token_transfers --start-block {latestSyncedBlock + 1} --end-block {latestBlock} --provider-uri {self.provider} --output {outputFile} --tokens {tokenAddress}"
          os.system(cmd)

          transfersFile = NetworkProvider.LOCAL_STORAGES[address]['TRANSFERS']

          if (os.path.exists(outputFile)):
            if os.path.getsize(outputFile) > 0:
              if (os.path.exists(transfersFile)):
                utils.csvreader.concatCsvs([transfersFile, outputFile], transfersFile)
              else:
                utils.csvreader.concatCsvs([outputFile], transfersFile)
            utils.jsonprovider.writeJson(NetworkProvider.LOCAL_STORAGES[address]['TRANSFERS_CACHE'], {"lastBlock" : latestBlock})
            os.remove(outputFile)
            print(f"{address} transfers synced")
          else:
            print("error occured")      
        else:
          print(f"{address} transfers already synced")
      print("All transfers are synced now")

    def getTokenBalances(self, address, blocks, treshhold):     
      transfers, _ = utils.csvreader.readCsv([NetworkProvider.LOCAL_STORAGES[address]['TRANSFERS']])

      blockAccounts = []
      lastTransferIndex = 0
      for block in blocks:
        accounts = copy.copy(blockAccounts[len(blockAccounts) - 1]) if (len(blockAccounts)) > 0 else {}
        for txIndex in range(lastTransferIndex, len(transfers)):
          tx = transfers[txIndex]
          if int(tx['block_number']) <= int(blocks[block][0]['number']):
            val = int(tx['value'])
            if tx['from_address'] in accounts:
              accounts[tx['from_address']] -= val
            else:
              accounts[tx['from_address']] = -val
            if tx['to_address'] in accounts:
              accounts[tx['to_address']] += val
            else:
              accounts[tx['to_address']] = val
          else:
            lastTransferIndex = txIndex
            break
        blockAccounts.append(accounts)

    
      #get all accounts
      accounts = []
      for accountsBlock in blockAccounts:
          keys = list(accountsBlock.keys())
          accounts.extend(keys)
      accounts = list(set(accounts))
      accounts.sort()

      #get all accounts balances over time
      data = []
      for account in accounts:
          accountData = []
          for accountsBlock in blockAccounts:
              if account in accountsBlock and accountsBlock[account] > 0:             
                accountData.append(self.w3.fromWei(accountsBlock[account], 'ether'))
              else:
                accountData.append(0)
          data.append(accountData)
      
      #prepare to export accounts

      #TO INPUTS
      otherBalances = []
      resultAccounts = []
      resultBalances = []
      for i in range(len(accounts)):
          if np.max(data[i]) >= treshhold or treshhold <= 0:
              resultAccounts.append(accounts[i])
              resultBalances.append(data[i])
          else:
              otherBalances = np.add(otherBalances, data[i]) if len(otherBalances) > 0 else data[i]
      if treshhold > 0:
          resultAccounts.append('others')
          resultBalances.append(otherBalances.tolist() if len(otherBalances) > 0 else [])
      
      return resultAccounts, resultBalances

