import json

from blockchaindata.jobs.base_job import BaseJob
from etherdata.executors.batch_work_executor import BatchWorkExecutor
from etherdata.utility.json_rpc_requests import generate_get_receipt_json_rpc
from etherdata.mappers.receipt_log_mapper import EthReceiptLogMapper
from etherdata.mappers.receipt_mapper import EthReceiptMapper
from etherdata.utility.utils import rpc_response_batch_to_results
from axel import Event

# Exports receipts and logs
class ExportReceiptsJob(BaseJob):
    def __init__(
            self,
            transaction_hashes_iterable,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_receipts=True,
            export_logs=True):
        self.batch_web3_provider = batch_web3_provider
        self.transaction_hashes_iterable = transaction_hashes_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_receipts = export_receipts
        self.export_logs = export_logs
        if not self.export_receipts and not self.export_logs:
            raise ValueError('At least one of export_receipts or export_logs must be True')

        self.receipt_mapper = EthReceiptMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()

        self.export_all = Event(self)
        self.load_all = Event(self)
        self.transform_all = Event(self)
        self.export = Event(self)
        self.load = Event(self)
        self.transform = Event(self)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.transaction_hashes_iterable, self._export_receipts)
        self.export_all('export_receipts')

    def _export_receipts(self, transaction_hashes):
        receipts_rpc = list(generate_get_receipt_json_rpc(transaction_hashes))
        response = self.batch_web3_provider.make_batch_request(json.dumps(receipts_rpc))
        results = rpc_response_batch_to_results(response)
        receipts = [self.receipt_mapper.json_dict_to_receipt(result) for result in results]
        for receipt in receipts:
            self._export_receipt(receipt)

    def _export_receipt(self, receipt):
        if self.export_receipts:
            self.item_exporter.export_item(self.receipt_mapper.receipt_to_dict(receipt))
        if self.export_logs:
            for log in receipt.logs:
                self.item_exporter.export_item(self.receipt_log_mapper.receipt_log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
