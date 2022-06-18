
import logging

from blockchaindata.utility.atomic_counter import AtomicCounter
from blockchaindata.utility.exporters import CsvItemExporter, JsonLinesItemExporter
from blockchaindata.utility.file_utils import get_file_handle, close_silently
from blockchaindata.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class CompositeItemExporter:
    def __init__(self, filename_mapping, field_mapping=None, converters=()):
        self.filename_mapping = filename_mapping
        self.field_mapping = field_mapping or {}

        self.file_mapping = {}
        self.exporter_mapping = {}
        self.counter_mapping = {}

        self.converter = CompositeItemConverter(converters)

        self.logger = logging.getLogger('CompositeItemExporter')

    def open(self):
        for item_type, filename in self.filename_mapping.items():
            file = get_file_handle(filename, binary=True)
            fields = self.field_mapping.get(item_type)
            self.file_mapping[item_type] = file
            if str(filename).endswith('.json'):
                item_exporter = JsonLinesItemExporter(file, fields_to_export=fields)
            else:
                item_exporter = CsvItemExporter(file, fields_to_export=fields)
            self.exporter_mapping[item_type] = item_exporter

            self.counter_mapping[item_type] = AtomicCounter()

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is None:
            raise ValueError('"type" key is not found in item {}'.format(repr(item)))

        exporter = self.exporter_mapping.get(item_type)
        if exporter is None:
            raise ValueError('Exporter for item type {} not found'.format(item_type))
        exporter.export_item(self.converter.convert_item(item))

        counter = self.counter_mapping.get(item_type)
        if counter is not None:
            counter.increment()

    def close(self):
        for item_type, file in self.file_mapping.items():
            close_silently(file)
            counter = self.counter_mapping[item_type]
            if counter is not None:
                self.logger.info('{} items exported: {}'.format(item_type, counter.increment() - 1))
