import click

from etherdata.jobs.export_geth_traces_job import ExportGethTracesJob
from etherdata.jobs.exporters.geth_traces_item_exporter import geth_traces_item_exporter
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.thread_local_proxy import ThreadLocalProxy

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to process at a time.')
@click.option('-o', '--output', default='-', show_default=True, type=str,
              help='The output file for geth traces. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', required=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or http://localhost:8545/')
def export_geth_traces(start_block, end_block, batch_size, output, max_workers, provider_uri):
    """Exports traces from geth node."""
    job = ExportGethTracesJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=geth_traces_item_exporter(output))

    job.run()
