class CompositeItemConverter:

    def __init__(self, converters=()):
        self.converters = converters

    def convert_item(self, item):
        if self.converters is None:
            return item

        for converter in self.converters:
            item = converter.convert_item(item)
        return item
