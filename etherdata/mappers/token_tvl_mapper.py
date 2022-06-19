class EthTokenTVLMapper(object):
    def token_tvl_to_dict(self, token_tvl):
        return {
            'type': 'token_tvl',
            'token_address': token_tvl.token_address,
            'value': token_tvl.value,
            'block_number': token_tvl.block_number,
        }
