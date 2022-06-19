

from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.utility.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from etherdata.mappers.trace_mapper import EthTraceMapper
from etherdata.service.eth_special_trace_service import EthSpecialTraceService

from etherdata.service.trace_id_calculator import calculate_trace_ids
from etherdata.service.trace_status_calculator import calculate_trace_statuses
from etherdata.utility.utils import validate_range
from axel import Event

class ExportTracesJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            web3,
            item_exporter,
            max_workers,
            include_genesis_traces=False,
            include_daofork_traces=False):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3

        self.batch_work_executor = BatchWorkExecutor(1, max_workers)
        self.item_exporter = item_exporter

        self.trace_mapper = EthTraceMapper()

        self.special_trace_service = EthSpecialTraceService()
        self.include_genesis_traces = include_genesis_traces
        self.include_daofork_traces = include_daofork_traces

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
        self.export_all('extract_parity_traces')

    def _export_batch(self, block_number_batch):
        assert len(block_number_batch) == 1
        block_number = block_number_batch[0]

        all_traces = []

        if self.include_genesis_traces and 0 in block_number_batch:
            genesis_traces = self.special_trace_service.get_genesis_traces()
            all_traces.extend(genesis_traces)

        if self.include_daofork_traces and DAOFORK_BLOCK_NUMBER in block_number_batch:
            daofork_traces = self.special_trace_service.get_daofork_traces()
            all_traces.extend(daofork_traces)

        json_traces = self.web3.parity.traceBlock(block_number)

        if json_traces is None:
            raise ValueError('Response from the node is None. Is the node fully synced? Is the node started with tracing enabled? Is trace_block API enabled?')

        traces = [self.trace_mapper.json_dict_to_trace(json_trace) for json_trace in json_traces]
        all_traces.extend(traces)

        calculate_trace_statuses(all_traces)
        calculate_trace_ids(all_traces)
        calculate_trace_indexes(all_traces)

        for trace in all_traces:
            self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()


def calculate_trace_indexes(traces):
    # Only works if traces were originally ordered correctly which is the case for Parity traces
    for ind, trace in enumerate(traces):
        trace.trace_index = ind
