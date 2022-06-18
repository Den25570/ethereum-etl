import json

from web3 import IPCProvider


class MockWeb3Provider(IPCProvider):
    def __init__(self, read_resource):
        self.read_resource = read_resource

    def make_request(self, method, params):
        file_name = build_file_name(method, params)
        file_content = self.read_resource(file_name)
        return json.loads(file_content)


def build_file_name(method, params):
    return 'web3_response.' + method + '_' + '_'.join([param_to_str(param) for param in params]) + '.json'


def param_to_str(param):
    if isinstance(param, dict):
        return '_'.join([str(key) + '_' + param_to_str(param[key]) for key in sorted(param)])
    elif isinstance(param, list):
        return '_'.join([param_to_str(param_item) for param_item in param])
    else:
        return str(param).lower()
