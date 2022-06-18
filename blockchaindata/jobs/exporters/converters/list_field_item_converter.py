class ListFieldItemConverter:

    def __init__(self, field, new_field_prefix, fill=0, fill_with=None):
        self.field = field
        self.new_field_prefix = new_field_prefix
        self.fill = fill
        self.fill_with = fill_with

    def convert_item(self, item):
        if not item:
            return item

        lst = item.get(self.field)
        result = item
        if lst is not None and isinstance(lst, list):
            result = item.copy()
            del result[self.field]
            for lst_item_index, lst_item in enumerate(lst):
                result[self.new_field_prefix + str(lst_item_index)] = lst_item
            if len(lst) < self.fill:
                for i in range(len(lst), self.fill):
                    result[self.new_field_prefix + str(i)] = self.fill_with
        return result
