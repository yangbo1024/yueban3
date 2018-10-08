# -*- coding:utf-8 -*-
"""
worker-逻辑进程，可以热更新
"""

import asyncio
from aiohttp import web
from . import utility
from . import communicate
from abc import ABCMeta
from abc import abstractmethod
from . import configuration
from . import log
from . import cache
from . import storage


_worker_app = None
_web_app = None
_worker_id = ""


class ProtocolMessage(object):
    def __init__(self, client_id, proto_id, body):
        self.client_id = client_id
        self.proto_id = proto_id
        self.body = body

    def __str__(self):
        return 'ProtocolMessage(client_id={1},proto_id={2},body={3}'.format(
            self.client_id, self.proto_id, self.body
        )


class Worker(object, metaclass=ABCMeta):
    @abstractmethod
    async def on_request(self, request):
        pass

    @abstractmethod
    async def on_schedule(self, name, args):
        pass

    @abstractmethod
    async def on_proto(self, message):
        """
        message是ProtocolMessage对象
        """
        pass

    @abstractmethod
    async def on_client_closed(self, client_id):
        pass


async def _yueban_handler(request):
    path = request.path
    if path == communicate.WorkerPath.Proto:
        data = await utility.unpack_pickle_request(request)
        client_id, path, body = data
        msg_obj = ProtocolMessage(client_id, path, body)
        await _worker_app.on_proto(msg_obj)
        return utility.pack_pickle_response('')
    elif path == communicate.WorkerPath.ClientClosed:
        data = await utility.unpack_pickle_request(request)
        client_id = data
        await _worker_app.on_client_closed(client_id)
        return utility.pack_pickle_response('')
    elif path == communicate.WorkerPath.OnSchedule:
        data = await utility.unpack_pickle_request(request)
        name, args = data
        await _worker_app.on_schedule(name, args)
        return utility.pack_pickle_response('')
    else:
        return await _worker_app.on_request(request)


async def unicast(client_id, proto_id, body):
    """
    单播
    """
    path = communicate.MasterPath.Proto
    master_id = communicate.get_master_id(client_id)
    await communicate.call_specific_master(master_id, path, [[client_id], proto_id, body])


async def multicast(client_ids, proto_id, body):
    """
    多播
    """
    if not client_ids:
        return
    client_ids = list(client_ids)
    path = communicate.MasterPath.Proto
    grouped_ids = {}
    for client_id in client_ids:
        master_id = communicate.get_master_id(client_id)
        ids = grouped_ids.setdefault(master_id, [])
        ids.append(client_id)
    tasks = []
    for master_id, mids in grouped_ids.items():
        task = communicate.call_specific_master(master_id, path, [mids, proto_id, body])
    await asyncio.wait(task)


async def broadcast(proto_id, body):
    """
    广播
    """
    path = communicate.MasterPath.Proto
    await communicate.call_all_masters(path, [[], proto_id, body])


async def close_client(client_id):
    """
    断开一个客户端
    """
    path = communicate.MasterPath.CloseClient
    master_id = communicate.get_master_id(client_id)
    await communicate.call_specific_master(master_id, path, client_id)


async def schedule_once(seconds, name, args):
    """
    延迟回调
    """
    path = communicate.MasterPath.Schedule
    await communicate.call_master(path, [seconds, name, args])


def get_web_app():
    return _web_app


def get_worker_app():
    return _worker_app


async def initialize(cfg, worker_app):
    global _worker_app
    global _web_app
    if not isinstance(worker_app, Worker):
        raise TypeError("bad worker instance type")
    _worker_app = worker_app
    _web_app = web.Application()
    _web_app.router.add_get("/{path:.*}", _yueban_handler)
    _web_app.router.add_post("/{path:.*}", _yueban_handler)
    configuration.set_config(cfg)
    await log.initialize()
    tasks = [
        cache.initialize(),
        storage.initialize(),
    ]
    await asyncio.wait(tasks)
