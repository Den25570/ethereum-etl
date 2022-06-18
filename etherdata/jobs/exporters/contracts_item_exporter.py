


from blockchaindata.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'address',
    'bytecode',
    'function_sighashes',
    'is_erc20',
    'is_erc721',
    'is_erc1155',
    'block_number',
]


def contracts_item_exporter(contracts_output):
    return CompositeItemExporter(
        filename_mapping={
            'contract': contracts_output
        },
        field_mapping={
            'contract': FIELDS_TO_EXPORT
        }
    )
