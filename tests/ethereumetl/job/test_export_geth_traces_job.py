import pytest

from etherdata.utility.web3_utils import build_web3
import tests.resources
from etherdata.jobs.export_geth_traces_job import ExportGethTracesJob
from etherdata.jobs.exporters.geth_traces_item_exporter import geth_traces_item_exporter
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import compare_lines_ignore_order, read_file

# use same resources for testing export/extract jobs
RESOURCE_GROUP = 'test_extract_geth_traces_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


@pytest.mark.parametrize("start_block,end_block,resource_group,web3_provider_type", [
    (1, 1, 'block_without_transactions', 'mock'),
    (1000690, 1000690, 'block_with_create', 'mock'),
    (1011973, 1011973, 'block_with_suicide', 'mock'),
    (1000000, 1000000, 'block_with_subtraces', 'mock'),
    (1000895, 1000895, 'block_with_error', 'mock'),
])
def test_export_geth_traces_job(tmpdir, start_block, end_block, resource_group, web3_provider_type):
    traces_output_file = str(tmpdir.join('actual_geth_traces.json'))

    job = ExportGethTracesJob(
        start_block=start_block, end_block=end_block, batch_size=1,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_web3_provider(web3_provider_type, lambda file: read_resource(resource_group, file), batch=True)
        ),
        max_workers=5,
        item_exporter=geth_traces_item_exporter(traces_output_file),
    )
    job.run()

    compare_lines_ignore_order(
        read_resource(resource_group, 'geth_traces.json'), read_file(traces_output_file)
    )
