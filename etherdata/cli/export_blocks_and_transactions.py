import click

from etherdata.jobs.export_blocks_job import ExportBlocksJob
from etherdata.jobs.exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from etherdata.utility.utils import check_classic_provider_uri

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to export at a time.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--blocks-output', default=None, show_default=True, type=str,
              help='The output file for blocks. If not provided blocks will not be exported. Use "-" for stdout')
@click.option('--transactions-output', default=None, show_default=True, type=str,
              help='The output file for transactions. '
                   'If not provided transactions will not be exported. Use "-" for stdout')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_blocks_and_transactions(start_block, end_block, batch_size, provider_uri, max_workers, blocks_output,
                                   transactions_output, chain='ethereum'):
    """Exports blocks and transactions."""
    provider_uri = check_classic_provider_uri(chain, provider_uri)
    if blocks_output is None and transactions_output is None:
        raise ValueError('Either --blocks-output or --transactions-output options must be provided')

    job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=blocks_and_transactions_item_exporter(blocks_output, transactions_output),
        export_blocks=blocks_output is not None,
        export_transactions=transactions_output is not None)
    job.run()
