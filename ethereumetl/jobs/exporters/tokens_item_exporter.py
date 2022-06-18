


from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'address',
    'symbol',
    'name',
    'decimals',
    'total_supply',
    'block_number'
]


def tokens_item_exporter(tokens_output, converters=()):
    return CompositeItemExporter(
        filename_mapping={
            'token': tokens_output
        },
        field_mapping={
            'token': FIELDS_TO_EXPORT
        },
        converters=converters
    )
