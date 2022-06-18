import pytest
import tests.resources
from etherdata.jobs.export_receipts_job import ExportReceiptsJob
from etherdata.jobs.exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from etherdata.utility.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import compare_lines_ignore_order, read_file, skip_if_slow_tests_disabled

RESOURCE_GROUP = 'test_export_receipts_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


DEFAULT_TX_HASHES = ['0x04cbcb236043d8fb7839e07bbc7f5eed692fb2ca55d897f1101eac3e3ad4fab8',
                     '0x463d53f0ad57677a3b430a007c1c31d15d62c37fab5eee598551697c297c235c',
                     '0x05287a561f218418892ab053adfb3d919860988b19458c570c5c30f51c146f02',
                     '0xcea6f89720cc1d2f46cc7a935463ae0b99dd5fad9c91bb7357de5421511cee49']


@pytest.mark.parametrize("batch_size,transaction_hashes,output_format,resource_group,web3_provider_type", [
    (1, DEFAULT_TX_HASHES, 'csv', 'receipts_with_logs', 'mock'),
    (2, DEFAULT_TX_HASHES, 'csv', 'receipts_with_logs', 'mock'),
    (2, DEFAULT_TX_HASHES, 'json', 'receipts_with_logs', 'mock'),
    skip_if_slow_tests_disabled((1, DEFAULT_TX_HASHES, 'csv', 'receipts_with_logs', 'infura')),
    skip_if_slow_tests_disabled((2, DEFAULT_TX_HASHES, 'json', 'receipts_with_logs', 'infura'))
])
def test_export_receipts_job(tmpdir, batch_size, transaction_hashes, output_format, resource_group, web3_provider_type):
    receipts_output_file = str(tmpdir.join('actual_receipts.' + output_format))
    logs_output_file = str(tmpdir.join('actual_logs.' + output_format))

    job = ExportReceiptsJob(
        transaction_hashes_iterable=transaction_hashes,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_web3_provider(web3_provider_type, lambda file: read_resource(resource_group, file), batch=True)
        ),
        max_workers=5,
        item_exporter=receipts_and_logs_item_exporter(receipts_output_file, logs_output_file),
        export_receipts=receipts_output_file is not None,
        export_logs=logs_output_file is not None
    )
    job.run()

    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_receipts.' + output_format), read_file(receipts_output_file)
    )

    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_logs.' + output_format), read_file(logs_output_file)
    )
