from blockchaindata.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'block_number',
    'transaction_hash',
    'transaction_index',
    'from_address',
    'to_address',
    'value',
    'input',
    'output',
    'trace_type',
    'call_type',
    'reward_type',
    'gas',
    'gas_used',
    'subtraces',
    'trace_address',
    'error',
    'status',
    'trace_id',
]


def traces_item_exporter(traces_output):
    return CompositeItemExporter(
        filename_mapping={
            'trace': traces_output
        },
        field_mapping={
            'trace': FIELDS_TO_EXPORT
        }
    )
