import json
import logging
from datetime import datetime


class EthItemTimestampCalculator:
    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get('type')

        if item_type == 'block' and item.get('timestamp') is not None:
            return epoch_seconds_to_rfc3339(item.get('timestamp'))
        elif item.get('block_timestamp') is not None:
            return epoch_seconds_to_rfc3339(item.get('block_timestamp'))

        logging.warning('item_timestamp for item {} is None'.format(json.dumps(item)))

        return None


def epoch_seconds_to_rfc3339(timestamp):
    return datetime.utcfromtimestamp(int(timestamp)).isoformat() + 'Z'
