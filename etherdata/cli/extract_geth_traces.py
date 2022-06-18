
import csv
import jsonimport click

from blockchaindata.utility.file_utils import smart_open
from etherdata.jobs.exporters.traces_item_exporter import traces_item_exporter
from etherdata.jobs.extract_geth_traces_job import ExtractGethTracesJob
from blockchaindata.utility.logging_utils import logging_basic_config

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--input', required=True, type=str, help='The JSON file containing geth traces.')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to filter at a time.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
def extract_geth_traces(input, batch_size, output, max_workers):
    """Extracts geth traces from JSON lines file."""
    with smart_open(input, 'r') as geth_traces_file:
        if input.endswith('.json'):
            traces_iterable = (json.loads(line) for line in geth_traces_file)
        else:
            traces_iterable = (trace for trace in csv.DictReader(geth_traces_file))
        job = ExtractGethTracesJob(
            traces_iterable=traces_iterable,
            batch_size=batch_size,
            max_workers=max_workers,
            item_exporter=traces_item_exporter(output))

        job.run()