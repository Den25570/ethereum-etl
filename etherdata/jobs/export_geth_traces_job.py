import json

from etherdata.executors.batch_work_executor import BatchWorkExecutor
from etherdata.utility.json_rpc_requests import generate_trace_block_by_number_json_rpc
from blockchaindata.jobs.base_job import BaseJob
from etherdata.mappers.geth_trace_mapper import EthGethTraceMapper
from etherdata.utility.utils import validate_range, rpc_response_to_result
from axel import Event


# Exports geth traces
class ExportGethTracesJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.geth_trace_mapper = EthGethTraceMapper()

        self.export_all = Event(self)
        self.load_all = Event(self)
        self.transform_all = Event(self)
        self.export = Event(self)
        self.load = Event(self)
        self.transform = Event(self)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )
        self.export_all('export_geth_traces')

    def _export_batch(self, block_number_batch):
        trace_block_rpc = list(generate_trace_block_by_number_json_rpc(block_number_batch))
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_block_rpc))

        for response_item in response:
            block_number = response_item.get('id')
            result = rpc_response_to_result(response_item)

            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace({
                'block_number': block_number,
                'transaction_traces': [tx_trace.get('result') for tx_trace in result],
            })

            self.item_exporter.export_item(self.geth_trace_mapper.geth_trace_to_dict(geth_trace))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
