

from blockchaindata.utility.logging_utils import logging_basic_config
logging_basic_config()

import click

from etherdata.cli.calculate_tvl import calculate_tvl
from etherdata.cli.export_all import export_all
from etherdata.cli.export_blocks_and_transactions import export_blocks_and_transactions
from etherdata.cli.export_contracts import export_contracts
from etherdata.cli.export_geth_traces import export_geth_traces
from etherdata.cli.export_receipts_and_logs import export_receipts_and_logs
from etherdata.cli.export_token_transfers import export_token_transfers
from etherdata.cli.export_tokens import export_tokens
from etherdata.cli.export_traces import export_traces
from etherdata.cli.extract_contracts import extract_contracts
from etherdata.cli.extract_csv_column import extract_csv_column
from etherdata.cli.extract_field import extract_field
from etherdata.cli.extract_geth_traces import extract_geth_traces
from etherdata.cli.extract_token_transfers import extract_token_transfers
from etherdata.cli.extract_tokens import extract_tokens
from etherdata.cli.filter_items import filter_items
from etherdata.cli.get_block_range_for_date import get_block_range_for_date
from etherdata.cli.get_block_range_for_timestamps import get_block_range_for_timestamps
from etherdata.cli.get_keccak_hash import get_keccak_hash
from etherdata.cli.stream import stream


@click.group()
@click.version_option(version='2.0.2')
@click.pass_context
def cli(ctx):
    pass


# export
cli.add_command(export_all, "export_all")
cli.add_command(export_blocks_and_transactions, "export_blocks_and_transactions")
cli.add_command(calculate_tvl, "calculate_tvl")
cli.add_command(export_receipts_and_logs, "export_receipts_and_logs")
cli.add_command(export_token_transfers, "export_token_transfers")
cli.add_command(extract_token_transfers, "extract_token_transfers")
cli.add_command(export_contracts, "export_contracts")
cli.add_command(export_tokens, "export_tokens")
cli.add_command(export_traces, "export_traces")
cli.add_command(export_geth_traces, "export_geth_traces")
cli.add_command(extract_geth_traces, "extract_geth_traces")
cli.add_command(extract_contracts, "extract_contracts")
cli.add_command(extract_tokens, "extract_tokens")

# streaming
cli.add_command(stream, "stream")

# utils
cli.add_command(get_block_range_for_date, "get_block_range_for_date")
cli.add_command(get_block_range_for_timestamps, "get_block_range_for_timestamps")
cli.add_command(get_keccak_hash, "get_keccak_hash")
cli.add_command(extract_csv_column, "extract_csv_column")
cli.add_command(filter_items, "filter_items")
cli.add_command(extract_field, "extract_field")
