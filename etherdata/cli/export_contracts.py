import click

from blockchaindata.utility.file_utils import smart_open
from etherdata.jobs.export_contracts_job import ExportContractsJob
from etherdata.jobs.exporters.contracts_item_exporter import contracts_item_exporter
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.utils import check_classic_provider_uri

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to filter at a time.')
@click.option('-ca', '--contract-addresses', required=True, type=str,
              help='The file containing contract addresses, one per line.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_contracts(batch_size, contract_addresses, output, max_workers, provider_uri, chain='ethereum'):
    """Exports contracts bytecode and sighashes."""
    check_classic_provider_uri(chain, provider_uri)
    with smart_open(contract_addresses, 'r') as contract_addresses_file:
        contract_addresses = (contract_address.strip() for contract_address in contract_addresses_file
                              if contract_address.strip())
        job = ExportContractsJob(
            contract_addresses_iterable=contract_addresses,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            item_exporter=contracts_item_exporter(output),
            max_workers=max_workers)

        job.run()
