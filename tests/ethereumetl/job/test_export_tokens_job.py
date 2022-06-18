import pytest
from etherdata.utility.web3_utils import build_web3
import tests.resources
from etherdata.jobs.export_tokens_job import ExportTokensJob
from etherdata.jobs.exporters.tokens_item_exporter import tokens_item_exporter
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import compare_lines_ignore_order, read_file, skip_if_slow_tests_disabled

RESOURCE_GROUP = 'test_export_tokens_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


@pytest.mark.parametrize("token_addresses,resource_group,web3_provider_type", [
    (['0xf763be8b3263c268e9789abfb3934564a7b80054'], 'token_with_invalid_data', 'mock'),
    (['0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'], 'token_with_alternative_return_type', 'mock'),
    skip_if_slow_tests_disabled(
        (['0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0'], 'token_with_alternative_return_type', 'infura')
    )
])
def test_export_tokens_job(tmpdir, token_addresses, resource_group, web3_provider_type):
    output_file = str(tmpdir.join('tokens.csv'))

    job = ExportTokensJob(
        token_addresses_iterable=token_addresses,
        web3=ThreadLocalProxy(
            lambda: build_web3(get_web3_provider(web3_provider_type, lambda file: read_resource(resource_group, file)))
        ),
        item_exporter=tokens_item_exporter(output_file),
        max_workers=5
    )
    job.run()

    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_tokens.csv'), read_file(output_file)
    )
