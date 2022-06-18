

import contextlib
import csv
import json

import six

from ethereumetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import get_file_handle, smart_open


@contextlib.contextmanager
def get_item_iterable(input_file):
    fh = get_file_handle(input_file, 'r')

    if input_file.endswith('.csv'):
        set_max_field_size_limit()
        reader = csv.DictReader(fh)
    else:
        reader = (json.loads(line) for line in fh)

    try:
        yield reader
    finally:
        fh.close()


@contextlib.contextmanager
def get_item_sink(output_file):
    fh = get_file_handle(output_file, 'w')

    if output_file.endswith('.csv'):
        set_max_field_size_limit()

        writer = None

        def sink(item):
            nonlocal writer
            if writer is None:
                fields = list(six.iterkeys(item))
                writer = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
                writer.writeheader()
            writer.writerow(item)
    else:
        def sink(item):
            fh.write(json.dumps(item) + '\n')

    try:
        yield sink
    finally:
        fh.close()


def filter_items(input_file, output_file, predicate):
    with get_item_iterable(input_file) as item_iterable, get_item_sink(output_file) as sink:
        for item in item_iterable:
            if predicate(item):
                sink(item)


def extract_field(input_file, output_file, field):
    with get_item_iterable(input_file) as item_iterable, smart_open(output_file, 'w') as output:
        for item in item_iterable:
            output.write(item[field] + '\n')
