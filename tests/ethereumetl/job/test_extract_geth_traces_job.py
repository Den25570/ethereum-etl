import json
import pytest
import tests.resources
from ethereumetl.jobs.exporters.traces_item_exporter import traces_item_exporter
from ethereumetl.jobs.extract_geth_traces_job import ExtractGethTracesJob
from tests.helpers import compare_lines_ignore_order, read_file

RESOURCE_GROUP = 'test_extract_geth_traces_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


@pytest.mark.parametrize('resource_group', [
    'block_without_transactions',
    'block_with_create',
    'block_with_suicide',
    'block_with_subtraces',
    'block_with_error',
])
def test_extract_traces_job(tmpdir, resource_group):
    output_file = str(tmpdir.join('actual_traces.csv'))

    geth_traces_content = read_resource(resource_group, 'geth_traces.json')
    traces_iterable = (json.loads(line) for line in geth_traces_content.splitlines())
    job = ExtractGethTracesJob(
        traces_iterable=traces_iterable,
        batch_size=2,
        item_exporter=traces_item_exporter(output_file),
        max_workers=5
    )
    job.run()

    print('=====================')
    print(read_file(output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_traces.csv'), read_file(output_file)
    )
