import json
import socket

from web3.providers.ipc import IPCProvider
from web3._utils.threads import (
    Timeout,
)

try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

class BatchIPCProvider(IPCProvider):
    _socket = None

    def make_batch_request(self, text):
        request = text.encode('utf-8')
        with self._lock, self._socket as sock:
            try:
                sock.sendall(request)
            except BrokenPipeError:
                # one extra attempt, then give up
                sock = self._socket.reset()
                sock.sendall(request)

            raw_response = b""
            with Timeout(self.timeout) as timeout:
                while True:
                    try:
                        raw_response += sock.recv(4096)
                    except socket.timeout:
                        timeout.sleep(0)
                        continue
                    if raw_response == b"":
                        timeout.sleep(0)
                    elif has_valid_json_rpc_ending(raw_response):
                        try:
                            response = json.loads(raw_response.decode('utf-8'))
                        except JSONDecodeError:
                            timeout.sleep(0)
                            continue
                        else:
                            return response
                    else:
                        timeout.sleep(0)
                        continue


# A valid JSON RPC response can only end in } or ] http://www.jsonrpc.org/specification
def has_valid_json_rpc_ending(raw_response):
    for valid_ending in [b"}\n", b"]\n"]:
        if raw_response.endswith(valid_ending):
            return True
    else:
        return False
