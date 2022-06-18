


import click
import csv

from ethereumetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import smart_open


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--input', default='-', show_default=True, type=str, help='The input file. If not specified stdin is used.')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-c', '--column', required=True, type=str, help='The csv column name to extract.')
def extract_csv_column(input, output, column):
    """Extracts column from given CSV file. Deprecated - use extract_field."""
    set_max_field_size_limit()

    with smart_open(input, 'r') as input_file, smart_open(output, 'w') as output_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            output_file.write(row[column] + '\n')
