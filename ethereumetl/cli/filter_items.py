

import click

from ethereumetl import misc_utils


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--input', default='-', show_default=True, type=str, help='The input file. If not specified stdin is used.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-p', '--predicate', required=True, type=str,
              help='Predicate in Python code e.g. "item[\'is_erc20\']".')
def filter_items(input, output, predicate):
    """Filters rows in given CSV or JSON newline-delimited file."""
    def evaluated_predicate(item):
        return eval(predicate, globals(), {'item': item})
    misc_utils.filter_items(input, output, evaluated_predicate)
