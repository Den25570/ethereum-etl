


from etherdata.mappers.trace_mapper import EthTraceMapper


class EthSpecialTraceService(object):

    def __init__(self):
        self.trace_mapper = EthTraceMapper()

    def get_genesis_traces(self):
        from etherdata.utility.mainnet_genesis_alloc import MAINNET_GENESIS_ALLOC
        genesis_traces = [self.trace_mapper.genesis_alloc_to_trace(alloc)
                          for alloc in MAINNET_GENESIS_ALLOC]
        return genesis_traces

    def get_daofork_traces(self):
        from etherdata.utility.mainnet_daofork_state_changes import MAINNET_DAOFORK_STATE_CHANGES
        daofork_traces = [self.trace_mapper.daofork_state_change_to_trace(change)
                          for change in MAINNET_DAOFORK_STATE_CHANGES]
        return daofork_traces
