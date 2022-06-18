import click

from etherdata.utility.web3_utils import build_web3

from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.jobs.export_origin_job import ExportOriginJob
from etherdata.jobs.exporters.origin_exporter import origin_marketplace_listing_item_exporter, origin_shop_product_item_exporter
from etherdata.ipfs.origin import get_origin_ipfs_client
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.thread_local_proxy import ThreadLocalProxy

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to filter at a time.')
@click.option('--marketplace-output', default='-', show_default=True, type=str, help='The output file for marketplace data. If not specified stdout is used.')
@click.option('--shop-output', default='-', show_default=True, type=str, help='The output file for shop data. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', required=True, type=str,
              help='The URI of the web3 provider e.g. file://$HOME/Library/Ethereum/geth.ipc or http://localhost:8545/')
def export_origin(start_block, end_block, batch_size, marketplace_output, shop_output, max_workers, provider_uri):
    """Exports Origin Protocol data."""
    job = ExportOriginJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri))),
        ipfs_client=get_origin_ipfs_client(),
        marketplace_listing_exporter=origin_marketplace_listing_item_exporter(marketplace_output),
        shop_product_exporter=origin_shop_product_item_exporter(shop_output),
        max_workers=max_workers)
    job.run()