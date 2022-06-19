from blockchaindata.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'block_number',
    'token_address',
    'value',
]


def token_tvl_item_exporter(token_tvl_output, converters=()):
    return CompositeItemExporter(
        filename_mapping={
            'token_tvl': token_tvl_output
        },
        field_mapping={
            'token_tvl': FIELDS_TO_EXPORT
        },
        converters=converters
    )
