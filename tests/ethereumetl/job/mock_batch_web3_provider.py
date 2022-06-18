


import json

from tests.ethereumetl.job.mock_web3_provider import MockWeb3Provider, build_file_name


class MockBatchWeb3Provider(MockWeb3Provider):

    def __init__(self, read_resource):
        super().__init__(read_resource)
        self.read_resource = read_resource

    def make_batch_request(self, text):
        batch = json.loads(text)
        web3_response = []
        for req in batch:
            method = req['method']
            params = req['params']
            file_name = build_file_name(method, params)
            file_content = self.read_resource(file_name)
            web3_response.append(json.loads(file_content))
        return web3_response
