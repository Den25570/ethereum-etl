from decimal import Decimal
from blockchaindata.jobs.exporters.converters.simple_item_converter import SimpleItemConverter

class IntToDecimalItemConverter(SimpleItemConverter):

    def convert_field(self, key, value):
        if isinstance(value, int):
            return Decimal(value)
        else:
            return value
