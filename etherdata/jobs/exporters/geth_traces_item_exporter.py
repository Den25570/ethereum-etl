from blockchaindata.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'block_number',
    'transaction_traces',
]


def geth_traces_item_exporter(geth_traces_output):
    return CompositeItemExporter(
        filename_mapping={
            'geth_trace': geth_traces_output
        },
        field_mapping={
            'geth_trace': FIELDS_TO_EXPORT
        }
    )
