from etherdata.domain.token_tvl import TokenTotalValueLocked
from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.mappers.token_transfer_mapper import EthTokenTransferMapper
from etherdata.mappers.token_tvl_mapper import EthTokenTVLMapper
from etherdata.service.tokens_processor import TokenProcessor
from axel import Event


class CalculateTokenTVLJob(BaseJob):
    def __init__(
            self,
            blocks_iterable,
            transfers_iterable,
            max_workers,
            item_exporter):
        self.transfers_iterable = transfers_iterable
        self.blocks_iterable = blocks_iterable

        self.batch_work_executor = BatchWorkExecutor(1, max_workers)
        self.item_exporter = item_exporter

        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_tvl_mapper = EthTokenTVLMapper()
        self.token_tvl_calculator = TokenProcessor()

        self.export_all = Event(self)
        self.load_all = Event(self)
        self.transform_all = Event(self)
        self.export = Event(self)
        self.load = Event(self)
        self.transform = Event(self)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        token_transfers = [self.token_transfer_mapper.dict_to_transfer(transfer_dict) for transfer_dict in self.transfers_iterable]
        tokens_list = self.token_tvl_calculator.get_tokens_by_transfers(token_transfers)
        transfers_per_token = []
        for token in tokens_list:
            transfers_per_token.append([transfer for transfer in token_transfers if transfer.token_address == token])
        self.load_all('calculate_tvl')
        
        self.batch_work_executor.execute(transfers_per_token, self._calculate_token_tvls)
        self.export_all('calculate_tvl')

    def _calculate_token_tvls(self, transfers_per_token):
        
        if transfers_per_token is not None:
            for transfers in transfers_per_token:
                blocks_list = self.blocks_iterable if len(self.blocks_iterable) > 0 else self.token_tvl_calculator.get_blocks_by_transfers(transfers)
                blocks_list.sort()
                tvls = self.token_tvl_calculator.get_total_value_locked(transfers, blocks_list)
                self.transform('calculate_tvl')
                if tvls is not None:
                    for i, tvl in enumerate(tvls):
                        tvl_obj = TokenTotalValueLocked()
                        tvl_obj.value = tvl
                        tvl_obj.block_number = blocks_list[i]
                        tvl_obj.token_address = transfers[0].token_address
                        self.item_exporter.export_item(self.token_tvl_mapper.token_tvl_to_dict(tvl_obj))
                self.export('calculate_tvl')

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
