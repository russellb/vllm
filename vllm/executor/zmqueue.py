from typing import Any, Optional

from zmq import Context, PUSH, PULL, PUB, SUB, SUBSCRIBE # type: ignore

import vllm.envs as envs
from vllm.logger import init_logger
import vllm.utils


logger = init_logger(__name__)


class QueueSender:
    def __init__(self):
        self._context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self._uuid = vllm.utils.random_uuid()
        self._socket_path = f"ipc:///{base_rpc_path}/{self._uuid}"
        self._sender = self._context.socket(PUSH)
        self._sender.bind(self._socket_path)

    @property
    def uuid(self):
        return self._uuid

    def put(self, item: Any):
        logger.info("QueueSender (%s) sending item: %s", self._socket_path, item)
        self._sender.send_pyobj(item)

    def close(self):
        logger.info("QueueSender (%s) closing", self._socket_path)
        self._sender.close()
        self._context.term()


class QueueReceiver:
    def __init__(self, uuid: Optional[str] = None):
        self._context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self._uuid = uuid or vllm.utils.random_uuid()
        self._socket_path = f"ipc:///{base_rpc_path}/{self._uuid}"
        self._receiver = self._context.socket(PULL)
        self._receiver.connect(self._socket_path)

    @property
    def uuid(self):
        return self._uuid

    def get(self):
        logger.info("QueueReceiver (%s) waiting for item", self._socket_path)
        obj = self._receiver.recv_pyobj()
        logger.info("QueueReceiver (%s) received item: %s", self._socket_path, obj)
        return obj

    def close(self, sentinel: str = ''):
        logger.info("QueueReceiver (%s) closing", self._socket_path)
        if sentinel:
            self._receiver.put(sentinel)
        self._receiver.close()
        self._context.term()


class Pub:
    def __init__(self, uuid: Optional[str] = None):
        self._context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self._uuid = uuid or vllm.utils.random_uuid()
        self._socket_path = f"ipc:///{base_rpc_path}/{self._uuid}"
        self._publisher = self._context.socket(PUB)
        self._publisher.connect(self._socket_path)

    @property
    def uuid(self):
        return self._uuid

    def put(self, item):
        logger.info("Pub (%s) sending item: %s", self._socket_path, item)
        self._publisher.send_pyobj(item)

    def close(self):
        logger.info("Pub (%s) closing", self._socket_path)
        self._publisher.close()
        self._context.term()


class Sub:
    def __init__(self):
        self._context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self._uuid = vllm.utils.random_uuid()
        self._socket_path = f"ipc:///{base_rpc_path}/{self._uuid}"
        self._subscriber = self._context.socket(SUB)
        self._subscriber.setsockopt_string(SUBSCRIBE, '')
        self._subscriber.bind(self._socket_path)

    @property
    def uuid(self):
        return self._uuid

    def get(self):
        logger.info("Sub (%s) waiting for item", self._socket_path)
        obj = self._subscriber.recv_pyobj()
        logger.info("Sub (%s) received item: %s", self._socket_path, obj)
        return obj

    def close(self):
        logger.info("Sub (%s) closing", self._socket_path)
        self._subscriber.close()
        self._context.term()
