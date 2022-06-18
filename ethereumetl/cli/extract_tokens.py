


import csv
import json

import click
from blockchainetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import smart_open
from blockchainetl.jobs.exporters.converters.int_to_string_item_converter import IntToStringItemConverter
from ethereumetl.jobs.exporters.tokens_item_exporter import tokens_item_exporter
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from blockchainetl.logging_utils import logging_basic_config
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--contracts', type=str, required=True, help='The JSON file containing contracts.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--values-as-strings', default=False, show_default=True, is_flag=True, help='Whether to convert values to strings.')
def extract_tokens(contracts, provider_uri, output, max_workers, values_as_strings=False):
    """Extracts tokens from contracts file."""

    set_max_field_size_limit()

    with smart_open(contracts, 'r') as contracts_file:
        if contracts.endswith('.json'):
            contracts_iterable = (json.loads(line) for line in contracts_file)
        else:
            contracts_iterable = csv.DictReader(contracts_file)
        converters = [IntToStringItemConverter(keys=['decimals', 'total_supply'])] if values_as_strings else []
        job = ExtractTokensJob(
            contracts_iterable=contracts_iterable,
            web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri))),
            max_workers=max_workers,
            item_exporter=tokens_item_exporter(output, converters))

        job.run()
