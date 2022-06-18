from blockchaindata.jobs.exporters.converters.simple_item_converter import SimpleItemConverter


class IntToStringItemConverter(SimpleItemConverter):

    def __init__(self, keys=None):
        self.keys = set(keys) if keys else None

    def convert_field(self, key, value):
        if isinstance(value, int) and (self.keys is None or key in self.keys):
            return str(value)
        else:
            return value
