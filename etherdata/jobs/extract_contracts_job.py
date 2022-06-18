


from etherdata.domain.contract import EthContract
from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.mappers.contract_mapper import EthContractMapper

from etherdata.service.eth_contract_service import EthContractService
from etherdata.utility.utils import to_int_or_none


# Extract contracts
class ExtractContractsJob(BaseJob):
    def __init__(
            self,
            traces_iterable,
            batch_size,
            max_workers,
            item_exporter):
        self.traces_iterable = traces_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.contract_service = EthContractService()
        self.contract_mapper = EthContractMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.traces_iterable, self._extract_contracts)

    def _extract_contracts(self, traces):
        for trace in traces:
            trace['status'] = to_int_or_none(trace.get('status'))
            trace['block_number'] = to_int_or_none(trace.get('block_number'))

        contract_creation_traces = [trace for trace in traces
                                    if trace.get('trace_type') == 'create' and trace.get('to_address') is not None
                                    and len(trace.get('to_address')) > 0 and trace.get('status') == 1]

        contracts = []
        for trace in contract_creation_traces:
            contract = EthContract()
            contract.address = trace.get('to_address')
            bytecode = trace.get('output')
            contract.bytecode = bytecode
            contract.block_number = trace.get('block_number')

            function_sighashes = self.contract_service.get_function_sighashes(bytecode)

            contract.function_sighashes = function_sighashes
            contract.is_erc20 = self.contract_service.is_erc20_contract(function_sighashes)
            contract.is_erc721 = self.contract_service.is_erc721_contract(function_sighashes)
            contract.is_erc1155 = self.contract_service.is_erc1155_contract(function_sighashes)

            contracts.append(contract)

        for contract in contracts:
            self.item_exporter.export_item(self.contract_mapper.contract_to_dict(contract))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
