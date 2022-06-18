


import pytest
from ethereumetl.web3_utils import build_web3

import tests.resources
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.exporters.token_transfers_item_exporter import token_transfers_item_exporter
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import compare_lines_ignore_order, read_file

RESOURCE_GROUP = 'test_export_token_transfers_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


@pytest.mark.parametrize("start_block,end_block,batch_size,resource_group,web3_provider_type", [
    (483920, 483920, 1, 'block_with_transfers', 'mock')
])
def test_export_token_transfers_job(tmpdir, start_block, end_block, batch_size, resource_group, web3_provider_type):
    output_file = str(tmpdir.join('token_transfers.csv'))

    job = ExportTokenTransfersJob(
        start_block=start_block, end_block=end_block, batch_size=batch_size,
        web3=ThreadLocalProxy(
            lambda: build_web3(get_web3_provider(web3_provider_type, lambda file: read_resource(resource_group, file)))
        ),
        item_exporter=token_transfers_item_exporter(output_file),
        max_workers=5
    )
    job.run()

    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_token_transfers.csv'), read_file(output_file)
    )
