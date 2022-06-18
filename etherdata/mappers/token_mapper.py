


class EthTokenMapper(object):
    def token_to_dict(self, token):
        return {
            'type': 'token',
            'address': token.address,
            'symbol': token.symbol,
            'name': token.name,
            'decimals': token.decimals,
            'total_supply': token.total_supply,
            'block_number': token.block_number
        }
