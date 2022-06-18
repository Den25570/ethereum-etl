


from blockchaindata.jobs.exporters.composite_item_exporter import CompositeItemExporter

RECEIPT_FIELDS_TO_EXPORT = [
    'transaction_hash',
    'transaction_index',
    'block_hash',
    'block_number',
    'cumulative_gas_used',
    'gas_used',
    'contract_address',
    'root',
    'status',
    'effective_gas_price'
]

LOG_FIELDS_TO_EXPORT = [
    'log_index',
    'transaction_hash',
    'transaction_index',
    'block_hash',
    'block_number',
    'address',
    'data',
    'topics'
]


def receipts_and_logs_item_exporter(receipts_output=None, logs_output=None):
    return CompositeItemExporter(
        filename_mapping={
            'receipt': receipts_output,
            'log': logs_output
        },
        field_mapping={
            'receipt': RECEIPT_FIELDS_TO_EXPORT,
            'log': LOG_FIELDS_TO_EXPORT
        }
    )
