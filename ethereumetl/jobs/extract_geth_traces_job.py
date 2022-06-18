

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.mappers.trace_mapper import EthTraceMapper
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper


class ExtractGethTracesJob(BaseJob):
    def __init__(
            self,
            traces_iterable,
            batch_size,
            max_workers,
            item_exporter):
        self.traces_iterable = traces_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.trace_mapper = EthTraceMapper()
        self.geth_trace_mapper = EthGethTraceMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.traces_iterable, self._extract_geth_traces)

    def _extract_geth_traces(self, geth_traces):
        for geth_trace_dict in geth_traces:
            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(geth_trace_dict)
            traces = self.trace_mapper.geth_trace_to_traces(geth_trace)
            for trace in traces:
                self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))
              
    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
