from zmq import Context, PUSH, PULL, PUB, SUB, SUBSCRIBE # type: ignore

import vllm.envs as envs
from vllm.logger import init_logger


logger = init_logger(__name__)


class QueueSender:
    def __init__(self, uuid: str):
        self.context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self.socket_path = f"ipc:///{base_rpc_path}/{uuid}"
        self.sender = self.context.socket(PUSH)
        self.sender.bind(self.socket_path)

    def put(self, item):
        logger.info("QueueSender (%s) sending item: %s", self.socket_path, item)
        self.sender.send_pyobj(item)

    def close(self):
        logger.info("QueueSender (%s) closing", self.socket_path)
        self.sender.close()
        self.context.term()


class QueueReceiver:
    def __init__(self, uuid: str):
        self.context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self.socket_path = f"ipc:///{base_rpc_path}/{uuid}"
        self.receiver = self.context.socket(PULL)
        self.receiver.connect(self.socket_path)

    def get(self):
        logger.info("QueueReceiver (%s) waiting for item", self.socket_path)
        obj = self.receiver.recv_pyobj()
        logger.info("QueueReceiver (%s) received item: %s", self.socket_path, obj)
        return obj

    def close(self, sentinel: str = ''):
        logger.info("QueueReceiver (%s) closing", self.socket_path)
        if sentinel:
            self.receiver.put(sentinel)
        self.receiver.close()
        self.context.term()


class Pub:
    def __init__(self, uuid: str):
        self.context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self.socket_path = f"ipc:///{base_rpc_path}/{uuid}"
        self.publisher = self.context.socket(PUB)
        self.publisher.connect(self.socket_path)

    def put(self, item):
        logger.info("Pub (%s) sending item: %s", self.socket_path, item)
        self.publisher.send_pyobj(item)

    def close(self):
        logger.info("Pub (%s) closing", self.socket_path)
        self.publisher.close()
        self.context.term()


class Sub:
    def __init__(self, uuid: str):
        self.context = Context()
        base_rpc_path = envs.VLLM_RPC_BASE_PATH
        self.socket_path = f"ipc:///{base_rpc_path}/{uuid}"
        self.subscriber = self.context.socket(SUB)
        self.subscriber.setsockopt_string(SUBSCRIBE, '')
        self.subscriber.bind(self.socket_path)

    def get(self):
        logger.info("Sub (%s) waiting for item", self.socket_path)
        obj = self.subscriber.recv_pyobj()
        logger.info("Sub (%s) received item: %s", self.socket_path, obj)
        return obj

    def close(self):
        logger.info("Sub (%s) closing", self.socket_path)
        self.subscriber.close()
        self.context.term()
