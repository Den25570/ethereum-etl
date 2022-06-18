import csv
import jsonimport click
from blockchaindata.utility.csv_utils import set_max_field_size_limit
from blockchaindata.utility.file_utils import smart_open
from etherdata.jobs.exporters.contracts_item_exporter import contracts_item_exporter
from etherdata.jobs.extract_contracts_job import ExtractContractsJob
from blockchaindata.utility.logging_utils import logging_basic_config

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-t', '--traces', type=str, required=True, help='The CSV file containing traces.')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to filter at a time.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
def extract_contracts(traces, batch_size, output, max_workers):
    """Extracts contracts from traces file."""

    set_max_field_size_limit()

    with smart_open(traces, 'r') as traces_file:
        if traces.endswith('.json'):
            traces_iterable = (json.loads(line) for line in traces_file)
        else:
            traces_iterable = csv.DictReader(traces_file)
        job = ExtractContractsJob(
            traces_iterable=traces_iterable,
            batch_size=batch_size,
            max_workers=max_workers,
            item_exporter=contracts_item_exporter(output))

        job.run()
