from etherdata.jobs.export_tokens_job import ExportTokensJob
from axel import Event

class ExtractTokensJob(ExportTokensJob):
    def __init__(self, web3, item_exporter, contracts_iterable, max_workers):
        super().__init__(web3, item_exporter, [], max_workers)
        self.contracts_iterable = contracts_iterable

    def _export(self):
        self.batch_work_executor.execute(self.contracts_iterable, self._export_tokens_from_contracts)
        self.export_all('extract_tokens')

    def _export_tokens_from_contracts(self, contracts):
        tokens = [contract for contract in contracts if contract.get('is_erc20') or contract.get('is_erc721') or contract.get('is_erc1155')]
        self.load('extract_tokens')
        for token in tokens:
            self._export_token(token_address=token['address'], block_number=token['block_number'])



