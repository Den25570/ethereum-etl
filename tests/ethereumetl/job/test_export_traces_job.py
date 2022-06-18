import pytest

from etherdata.utility.web3_utils import build_web3import tests.resources
from etherdata.jobs.export_traces_job import ExportTracesJob
from etherdata.jobs.exporters.traces_item_exporter import traces_item_exporter
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import compare_lines_ignore_order, read_file, skip_if_slow_tests_disabled

RESOURCE_GROUP = 'test_export_traces_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


@pytest.mark.parametrize("start_block,end_block,resource_group,web3_provider_type", [
    (0, 0, 'block_without_transactions', 'mock'),
    (1000690, 1000690, 'block_with_create', 'mock'),
    (1011973, 1011973, 'block_with_suicide', 'mock'),
    (1000000, 1000000, 'block_with_subtraces', 'mock'),
    (1000895, 1000895, 'block_with_error', 'mock'),
])
def test_export_traces_job(tmpdir, start_block, end_block, resource_group, web3_provider_type):
    traces_output_file = str(tmpdir.join('actual_traces.csv'))

    job = ExportTracesJob(
        start_block=start_block, end_block=end_block, batch_size=1,
        web3=ThreadLocalProxy(
            lambda: build_web3(get_web3_provider(web3_provider_type, lambda file: read_resource(resource_group, file)))
        ),
        max_workers=5,
        item_exporter=traces_item_exporter(traces_output_file),
    )
    job.run()

    print('=====================')
    print(read_file(traces_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_traces.csv'), read_file(traces_output_file)
    )
