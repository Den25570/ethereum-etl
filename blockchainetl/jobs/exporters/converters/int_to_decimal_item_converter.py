from decimal import Decimal

from blockchainetl.jobs.exporters.converters.simple_item_converter import SimpleItemConverter

# Large ints are not handled correctly by pg8000 so we use Decimal instead:
# https://github.com/mfenniak/pg8000/blob/412eace074514ada824e7a102765e37e2cda8eaa/pg8000/core.py#L1703
class IntToDecimalItemConverter(SimpleItemConverter):

    def convert_field(self, key, value):
        if isinstance(value, int):
            return Decimal(value)
        else:
            return value
