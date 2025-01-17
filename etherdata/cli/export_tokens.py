import click

from etherdata.utility.web3_utils import build_web3

from blockchaindata.utility.file_utils import smart_open
from etherdata.jobs.export_tokens_job import ExportTokensJob
from etherdata.jobs.exporters.tokens_item_exporter import tokens_item_exporter
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.utils import check_classic_provider_uri

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-t', '--token-addresses', required=True, type=str,
              help='The file containing token addresses, one per line.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_tokens(token_addresses, output, max_workers, provider_uri, chain='ethereum'):
    """Exports ERC20/ERC721 tokens."""
    provider_uri = check_classic_provider_uri(chain, provider_uri)
    with smart_open(token_addresses, 'r') as token_addresses_file:
        job = ExportTokensJob(
            token_addresses_iterable=(token_address.strip() for token_address in token_addresses_file),
            web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri))),
            item_exporter=tokens_item_exporter(output),
            max_workers=max_workers)

        job.run()
