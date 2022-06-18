import json
import logging


class EthItemIdCalculator:

    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get('type')

        if item_type == 'block' and item.get('hash') is not None:
            return concat(item_type, item.get('hash'))
        elif item_type == 'transaction' and item.get('hash') is not None:
            return concat(item_type, item.get('hash'))
        elif item_type == 'log' and item.get('transaction_hash') is not None and item.get('log_index') is not None:
            return concat(item_type, item.get('transaction_hash'), item.get('log_index'))
        elif item_type == 'token_transfer' and item.get('transaction_hash') is not None \
                and item.get('log_index') is not None:
            return concat(item_type, item.get('transaction_hash'), item.get('log_index'))
        elif item_type == 'trace' and item.get('trace_id') is not None:
            return concat(item_type, item.get('trace_id'))
        elif item_type == 'contract' and item.get('block_number') is not None and item.get('address') is not None:
            return concat(item_type, item.get('block_number'), item.get('address'))
        elif item_type == 'token' and item.get('block_number') is not None and item.get('address') is not None:
            return concat(item_type, item.get('block_number'), item.get('address'))

        logging.warning('item_id for item {} is None'.format(json.dumps(item)))

        return None


def concat(*elements):
    return '_'.join([str(elem) for elem in elements])
