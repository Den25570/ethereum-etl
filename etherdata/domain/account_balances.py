class AccountBalancesPerBlocks(object):
    def __init__(self, blocks, address, token_address, balances):
        self.blocks = blocks
        self.balances = balances
        self.address = address
        self.token_address = token_address

    
