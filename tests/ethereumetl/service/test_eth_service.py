
import os
import pytest
from dateutil.parser import parse
from web3 import HTTPProvider, Web3

from etherdata.service.eth_service import EthService
from etherdata.service.graph_operations import OutOfBoundsError
from etherdata.utility.web3_utils import build_web3
from tests.helpers import skip_if_slow_tests_disabled


@pytest.mark.parametrize("date,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled(['2015-07-30', 0, 6911]),
    skip_if_slow_tests_disabled(['2015-07-31', 6912, 13774]),
    skip_if_slow_tests_disabled(['2017-01-01', 2912407, 2918517]),
    skip_if_slow_tests_disabled(['2017-01-02', 2918518, 2924575]),
    skip_if_slow_tests_disabled(['2018-06-10', 5761663, 5767303])
])
def test_get_block_range_for_date(date, expected_start_block, expected_end_block):
    eth_service = get_new_eth_service()
    parsed_date = parse(date)
    blocks = eth_service.get_block_range_for_date(parsed_date)
    assert blocks == (expected_start_block, expected_end_block)


@pytest.mark.parametrize("date", [
    skip_if_slow_tests_disabled(['2015-07-29']),
    skip_if_slow_tests_disabled(['2030-01-01'])
])
def test_get_block_range_for_date_fail(date):
    eth_service = get_new_eth_service()
    parsed_date = parse(date)
    with pytest.raises(OutOfBoundsError):
        eth_service.get_block_range_for_date(parsed_date)


@pytest.mark.parametrize("start_timestamp,end_timestamp,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled([1438270128, 1438270128, 10, 10]),
    skip_if_slow_tests_disabled([1438270128, 1438270129, 10, 10])
])
def test_get_block_range_for_timestamps(start_timestamp, end_timestamp, expected_start_block, expected_end_block):
    eth_service = get_new_eth_service()
    blocks = eth_service.get_block_range_for_timestamps(start_timestamp, end_timestamp)
    assert blocks == (expected_start_block, expected_end_block)


@pytest.mark.parametrize("start_timestamp,end_timestamp", [
    skip_if_slow_tests_disabled([1438270129, 1438270131])
])
def test_get_block_range_for_timestamps_fail(start_timestamp, end_timestamp):
    eth_service = get_new_eth_service()
    with pytest.raises(ValueError):
        eth_service.get_block_range_for_timestamps(start_timestamp, end_timestamp)


def get_new_eth_service():
    provider_url = os.environ.get('PROVIDER_URL', 'https://mainnet.infura.io/v3/7aef3f0cd1f64408b163814b22cc643c')
    web3 = build_web3(HTTPProvider(provider_url))
    return EthService(web3)
