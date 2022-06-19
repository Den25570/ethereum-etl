import json

from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.utility.json_rpc_requests import generate_get_code_json_rpc
from etherdata.mappers.contract_mapper import EthContractMapper

from etherdata.service.eth_contract_service import EthContractService
from etherdata.utility.utils import rpc_response_to_result
from axel import Event

# Exports contracts bytecode
class ExportContractsJob(BaseJob):
    def __init__(
            self,
            contract_addresses_iterable,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter):
        self.batch_web3_provider = batch_web3_provider
        self.contract_addresses_iterable = contract_addresses_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.contract_service = EthContractService()
        self.contract_mapper = EthContractMapper()

        self.export_all = Event(self)
        self.load_all = Event(self)
        self.transform_all = Event(self)
        self.export = Event(self)
        self.load = Event(self)
        self.transform = Event(self)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.contract_addresses_iterable, self._export_contracts)
        self.export_all('export_contracts')

    def _export_contracts(self, contract_addresses):
        contracts_code_rpc = list(generate_get_code_json_rpc(contract_addresses))
        response_batch = self.batch_web3_provider.make_batch_request(json.dumps(contracts_code_rpc))

        contracts = []
        for response in response_batch:
            # request id is the index of the contract address in contract_addresses list
            request_id = response['id']
            result = rpc_response_to_result(response)

            contract_address = contract_addresses[request_id]
            contract = self._get_contract(contract_address, result)
            contracts.append(contract)

        self.load_all('export_contracts')

        for contract in contracts:
            self.item_exporter.export_item(self.contract_mapper.contract_to_dict(contract))

    def _get_contract(self, contract_address, rpc_result):
        contract = self.contract_mapper.rpc_result_to_contract(contract_address, rpc_result)
        bytecode = contract.bytecode
        function_sighashes = self.contract_service.get_function_sighashes(bytecode)

        contract.function_sighashes = function_sighashes
        contract.is_erc20 = self.contract_service.is_erc20_contract(function_sighashes)
        contract.is_erc721 = self.contract_service.is_erc721_contract(function_sighashes)
        contract.is_erc1155 = self.contract_service.is_erc1155_contract(function_sighashes)

        return contract

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
