


class EthTrace(object):
    def __init__(self):
        self.block_number = None
        self.transaction_hash = None
        self.transaction_index = None
        self.from_address = None
        self.to_address = None
        self.value = None
        self.input = None
        self.output = None
        self.trace_type = None
        self.call_type = None
        self.reward_type = None
        self.gas = None
        self.gas_used = None
        self.subtraces = 0
        self.trace_address = None
        self.error = None
        self.status = None
        self.trace_id = None
        self.trace_index = None
