import click
import csv
import json

from blockchaindata.utility.file_utils import smart_open
from blockchaindata.jobs.exporters.converters.int_to_string_item_converter import IntToStringItemConverter
from etherdata.jobs.calculate_token_tvl import CalculateTokenTVLJob
from blockchaindata.utility.logging_utils import logging_basic_config
from etherdata.jobs.exporters.token_tvl_item_exporter import token_tvl_item_exporter

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-t', '--transfers', type=str, required=True, help='The CSV file containing transfers.')
@click.option('-b', '--blocks', type=str, default='', help='Comma separated blocks to get tvl.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--values-as-strings', default=False, show_default=True, is_flag=True, help='Whether to convert values to strings.')
def calculate_tvl(output, transfers, blocks, max_workers, values_as_strings):
    with smart_open(transfers, 'r') as transfers_file:
        if transfers.endswith('.json'):
            transfer_reader = (json.loads(line) for line in transfers_file)
        else:
            transfer_reader = csv.DictReader(transfers_file)
        blocks_list = [block.strip() for block in blocks.split(',')] if blocks is not None and len(blocks) > 0 else []
        converters = [IntToStringItemConverter(keys=['value'])] if values_as_strings else []
        job = CalculateTokenTVLJob(
            blocks_iterable=blocks_list,
            transfers_iterable=transfer_reader,
            max_workers=max_workers,
            item_exporter=token_tvl_item_exporter(output, converters=converters))

        job.run()
