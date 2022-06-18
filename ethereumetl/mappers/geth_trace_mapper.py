


from ethereumetl.domain.geth_trace import EthGethTrace


class EthGethTraceMapper(object):
    def json_dict_to_geth_trace(self, json_dict):
        geth_trace = EthGethTrace()

        geth_trace.block_number = json_dict.get('block_number')
        geth_trace.transaction_traces = json_dict.get('transaction_traces')

        return geth_trace

    def geth_trace_to_dict(self, geth_trace):
        return {
            'type': 'geth_trace',
            'block_number': geth_trace.block_number,
            'transaction_traces': geth_trace.transaction_traces,
        }
