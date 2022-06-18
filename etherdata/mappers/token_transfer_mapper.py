


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
