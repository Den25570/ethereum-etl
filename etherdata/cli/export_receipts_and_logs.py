import click

from blockchaindata.utility.file_utils import smart_open
from etherdata.jobs.export_receipts_job import ExportReceiptsJob
from etherdata.jobs.exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from etherdata.providers.auto import get_provider_from_uri
from etherdata.utility.utils import check_classic_provider_uri

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of receipts to export at a time.')
@click.option('-t', '--transaction-hashes', required=True, type=str,
              help='The file containing transaction hashes, one per line.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--receipts-output', default=None, show_default=True, type=str,
              help='The output file for receipts. If not provided receipts will not be exported. Use "-" for stdout')
@click.option('--logs-output', default=None, show_default=True, type=str,
              help='The output file for receipt logs. '
                   'aIf not provided receipt logs will not be exported. Use "-" for stdout')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_receipts_and_logs(batch_size, transaction_hashes, provider_uri, max_workers, receipts_output, logs_output,
                             chain='ethereum'):
    """Exports receipts and logs."""
    provider_uri = check_classic_provider_uri(chain, provider_uri)
    with smart_open(transaction_hashes, 'r') as transaction_hashes_file:
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction_hash.strip() for transaction_hash in transaction_hashes_file),
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            max_workers=max_workers,
            item_exporter=receipts_and_logs_item_exporter(receipts_output, logs_output),
            export_receipts=receipts_output is not None,
            export_logs=logs_output is not None)

        job.run()
