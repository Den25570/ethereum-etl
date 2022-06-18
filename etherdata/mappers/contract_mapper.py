


from etherdata.domain.contract import EthContract


class EthContractMapper(object):

    def rpc_result_to_contract(self, contract_address, rpc_result):
        contract = EthContract()
        contract.address = contract_address
        contract.bytecode = rpc_result

        return contract

    def contract_to_dict(self, contract):
        return {
            'type': 'contract',
            'address': contract.address,
            'bytecode': contract.bytecode,
            'function_sighashes': contract.function_sighashes,
            'is_erc20': contract.is_erc20,
            'is_erc721': contract.is_erc721,
            'is_erc1155': contract.is_erc1155,
            'block_number': contract.block_number
        }
