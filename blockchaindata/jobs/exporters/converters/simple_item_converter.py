class SimpleItemConverter:

    def convert_item(self, item):
        return {
            key: self.convert_field(key, value) for key, value in item.items()
        }

    def convert_field(self, key, value):
        return value
