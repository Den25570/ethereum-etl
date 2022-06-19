import json

from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.utility.json_rpc_requests import generate_get_block_by_number_json_rpc
from etherdata.mappers.block_mapper import EthBlockMapper
from etherdata.mappers.transaction_mapper import EthTransactionMapper
from etherdata.utility.utils import rpc_response_batch_to_results, validate_range
from axel import Event


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

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
        self.export_all('export_blocks')

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]

        for block in blocks:
            self._export_block(block)
            self.export('export_blocks')

    def _export_block(self, block):
        if self.export_blocks:
            self.item_exporter.export_item(self.block_mapper.block_to_dict(block))
        if self.export_transactions:
            for tx in block.transactions:
                self.item_exporter.export_item(self.transaction_mapper.transaction_to_dict(tx))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
