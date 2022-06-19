from etherdata.domain.token_transfer import EthTokenTransfer

class EthTokenTransferMapper(object):
    def token_transfer_to_dict(self, token_transfer):
        return {
            'type': 'token_transfer',
            'token_address': token_transfer.token_address,
            'from_address': token_transfer.from_address,
            'to_address': token_transfer.to_address,
            'value': token_transfer.value,
            'transaction_hash': token_transfer.transaction_hash,
            'log_index': token_transfer.log_index,
            'block_number': token_transfer.block_number,
        }

    def dict_to_transfer(self, dict):
        transfer = EthTokenTransfer()

        transfer.token_address = dict.get('token_address')
        transfer.from_address = dict.get('from_address')
        transfer.to_address = dict.get('to_address')
        transfer.value = dict.get('value')
        transfer.transaction_hash = dict.get('transaction_hash')
        transfer.log_index = dict.get('log_index')
        transfer.block_number = dict.get('block_number')

        return transfer
