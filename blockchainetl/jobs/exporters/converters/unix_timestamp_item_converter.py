

from datetime import datetime

from blockchainetl.jobs.exporters.converters.simple_item_converter import SimpleItemConverter


class UnixTimestampItemConverter(SimpleItemConverter):

    def convert_field(self, key, value):
        if key is not None and key.endswith('timestamp'):
            return to_timestamp(value)
        else:
            return value


def to_timestamp(value):
    if isinstance(value, int):
        return datetime.utcfromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return value
