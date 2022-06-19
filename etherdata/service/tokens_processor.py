import logging
from builtins import map
import copy
import numpy as np
from web3 import Web3
import math

from etherdata.domain.account_balances import AccountBalancesPerBlocks

class TokenProcessor(object):

    def __init__(self):
        self._web3 = Web3()

    def get_tokens_by_transfers(self, transfers):
        return list(set([transfer.token_address for transfer in transfers]))

    def get_blocks_by_transfers(self, transfers):
        return list(set([int(transfer.block_number) for transfer in transfers]))

    def get_total_value_locked(self, transfers, blocks):
        balances_per_block = self.get_total_account_values(transfers, blocks, 0)
        #print(en(blocks), [balances.address for balances in balances_per_block])

        tvl_per_blocks = [0 for block in blocks]
        for i, block in enumerate(blocks):
            tvl_per_blocks[i] = np.sum([balances_in_block.balances[i] for balances_in_block in balances_per_block if balances_in_block.address != '0x0000000000000000000000000000000000000000'])
        print(tvl_per_blocks)
        return tvl_per_blocks

    def get_total_account_values(self, transfers, blocks, treshhold = 0.01):
        accounts_per_block = []
        last_transfer_index = 0
        for block in blocks:
            accounts = copy.copy(accounts_per_block[-1]) if len(accounts_per_block) > 0 else {}
            for txIndex in range(last_transfer_index, len(transfers)):
                tx = transfers[txIndex]
                if int(tx.block_number) <= int(block):
                    val = int(tx.value)
                    if tx.from_address in accounts:
                        accounts[tx.from_address] -= val
                    else:
                        accounts[tx.from_address] = -val
                    if tx.to_address in accounts:
                        accounts[tx.to_address] += val
                    else:
                        accounts[tx.to_address] = val
                else:
                    last_transfer_index = txIndex
                    break
            accounts_per_block.append(accounts)
        
        #get all accounts
        all_accounts = []
        for accounts_in_blocks in accounts_per_block:
            keys = list(accounts_in_blocks.keys())
            all_accounts.extend(keys)
        all_accounts = list(set(accounts))
        all_accounts.sort()

        #get all accounts balances over time
        account_balances_per_block = []
        for account in all_accounts:
            account_balance = []
            for accounts in accounts_per_block:
                if account in accounts and accounts[account] > 0:             
                    account_balance.append(self._web3.fromWei(accounts[account], 'ether'))
                else:
                    account_balance.append(0)
            account_balances_per_block.append(account_balance)
        
        #prepare to export accounts
        others_balances = []
        result_balances_per_block = []
        for i in range(len(all_accounts)):
            if np.max(account_balances_per_block[i]) > treshhold:
                result_balances_per_block.append(AccountBalancesPerBlocks(None, all_accounts[i], None, account_balances_per_block[i]))
            else:
                others_balances = np.add(others_balances, account_balances_per_block[i]) if len(others_balances) > 0 else account_balances_per_block[i]
        
        result_balances_per_block.append(AccountBalancesPerBlocks(None, None, None, others_balances if len(others_balances) > 0 else []))
        return result_balances_per_block

    def get_total_holders(self, transfers, blocks, treshhold = 0.01):
        total_holders_per_block = []
        last_transfer_index = 0
        for block in blocks:
            accounts = copy.copy(total_holders_per_block[-1]) if len(total_holders_per_block) > 0 else {}
            for txIndex in range(last_transfer_index, len(transfers)):
                tx = transfers[txIndex]
                if int(tx.block_number) <= int(block):
                    val = int(tx.value)
                    if tx.from_address not in accounts:
                        accounts[tx.from_address] -= self._web3.fromWei(val, 'ether')
                    else:
                        accounts[tx.from_address] = -self._web3.fromWei(val, 'ether')
                    if tx.to_address in accounts:
                        accounts[tx.to_address] += self._web3.fromWei(val, 'ether')
                    else:
                        accounts[tx.to_address] = self._web3.fromWei(val, 'ether')
                else:
                    last_transfer_index = txIndex
                    break
            total_holders_per_block.append(accounts)
        
        #get all accounts
        total_holders_per_block = [list(filter(lambda d: d > treshhold, accounts_in_block)) for accounts_in_block in total_holders_per_block]

        return total_holders_per_block

    def get_total_burned(self, transfers, blocks):
        total_burned_per_block = []
        last_transfer_index = 0
        for block in blocks:
            burned = total_burned_per_block[-1] if len(total_burned_per_block) > 0 else 0
            for txIndex in range(last_transfer_index, len(transfers)):
                tx = transfers[txIndex]
                if int(tx.block_number) <= int(block):
                    if tx.to_address == '0x0000000000000000000000000000000000000000':
                        burned += self._web3.fromWei(int(tx.value), 'ether')
                else:
                    last_transfer_index = txIndex
                    break
            total_burned_per_block.append(burned)
        
        return total_burned_per_block   