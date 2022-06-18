import click

from etherdata.utility.web3_utils import build_web3

from blockchaindata.utility.file_utils import smart_open
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.providers.auto import get_provider_from_uri
from etherdata.service.eth_service import EthService
from etherdata.utility.utils import check_classic_provider_uri

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-s', '--start-timestamp', required=True, type=int, help='Start unix timestamp, in seconds.')
@click.option('-e', '--end-timestamp', required=True, type=int, help='End unix timestamp, in seconds.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def get_block_range_for_timestamps(provider_uri, start_timestamp, end_timestamp, output, chain='ethereum'):
    """Outputs start and end blocks for given timestamps."""
    provider_uri = check_classic_provider_uri(chain, provider_uri)
    provider = get_provider_from_uri(provider_uri)
    web3 = build_web3(provider)
    eth_service = EthService(web3)

    start_block, end_block = eth_service.get_block_range_for_timestamps(start_timestamp, end_timestamp)

    with smart_open(output, 'w') as output_file:
        output_file.write('{},{}\n'.format(start_block, end_block))