from etherdata.domain.receipt import EthReceipt
from etherdata.mappers.receipt_log_mapper import EthReceiptLogMapper
from etherdata.utility.utils import hex_to_dec, to_normalized_address


class EthReceiptMapper(object):
    def __init__(self, receipt_log_mapper=None):
        if receipt_log_mapper is None:
            self.receipt_log_mapper = EthReceiptLogMapper()
        else:
            self.receipt_log_mapper = receipt_log_mapper

    def json_dict_to_receipt(self, json_dict):
        receipt = EthReceipt()

        receipt.transaction_hash = json_dict.get('transactionHash')
        receipt.transaction_index = hex_to_dec(json_dict.get('transactionIndex'))
        receipt.block_hash = json_dict.get('blockHash')
        receipt.block_number = hex_to_dec(json_dict.get('blockNumber'))
        receipt.cumulative_gas_used = hex_to_dec(json_dict.get('cumulativeGasUsed'))
        receipt.gas_used = hex_to_dec(json_dict.get('gasUsed'))

        receipt.contract_address = to_normalized_address(json_dict.get('contractAddress'))

        receipt.root = json_dict.get('root')
        receipt.status = hex_to_dec(json_dict.get('status'))

        receipt.effective_gas_price = hex_to_dec(json_dict.get('effectiveGasPrice'))

        if 'logs' in json_dict:
            receipt.logs = [
                self.receipt_log_mapper.json_dict_to_receipt_log(log) for log in json_dict['logs']
            ]

        return receipt

    def receipt_to_dict(self, receipt):
        return {
            'type': 'receipt',
            'transaction_hash': receipt.transaction_hash,
            'transaction_index': receipt.transaction_index,
            'block_hash': receipt.block_hash,
            'block_number': receipt.block_number,
            'cumulative_gas_used': receipt.cumulative_gas_used,
            'gas_used': receipt.gas_used,
            'contract_address': receipt.contract_address,
            'root': receipt.root,
            'status': receipt.status,
            'effective_gas_price': receipt.effective_gas_price
        }
