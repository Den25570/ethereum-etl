from etherdata.executors.batch_work_executor import BatchWorkExecutor
from blockchaindata.jobs.base_job import BaseJob
from etherdata.mappers.trace_mapper import EthTraceMapper
from etherdata.mappers.geth_trace_mapper import EthGethTraceMapper
from axel import Event

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

        self.export_all = Event(self)
        self.load_all = Event(self)
        self.transform_all = Event(self)
        self.export = Event(self)
        self.load = Event(self)
        self.transform = Event(self)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.traces_iterable, self._extract_geth_traces)
        self.export_all('extract_geth_traces')

    def _extract_geth_traces(self, geth_traces):
        for geth_trace_dict in geth_traces:
            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(geth_trace_dict)
            traces = self.trace_mapper.geth_trace_to_traces(geth_trace)
            for trace in traces:
                self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))
              
    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
