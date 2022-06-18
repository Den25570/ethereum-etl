


import click

from eth_utils import keccak

from blockchainetl.file_utils import smart_open
from blockchainetl.logging_utils import logging_basic_config


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--input-string', default='Transfer(address,address,uint256)', show_default=True, type=str,
              help='String to hash, e.g. Transfer(address,address,uint256)')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
def get_keccak_hash(input_string, output):
    """Outputs 32-byte Keccak hash of given string."""
    hash = keccak(text=input_string)

    with smart_open(output, 'w') as output_file:
        output_file.write('0x{}\n'.format(hash.hex()))
