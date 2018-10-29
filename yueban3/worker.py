# -*- coding:utf-8 -*-
"""
worker-逻辑进程
通过gunicorn启动，
可以热更新
"""

import asyncio
import json
from aiohttp import web
from . import utility
from . import communicate
from abc import ABCMeta
from abc import abstractmethod
import traceback
from . import configuration
from . import log
from . import cache
from . import storage
from . import table


_worker_app = None
_web_app = None
_worker_id = ""
_grace_timeout = 5


class ProtocolMessage(object):
    """
    长连接协议收到的数据包封装对象
    data可以是二进制也可能是文本
    """
    def __init__(self, host, client_id, msg_type, data):
        self.host = host
        self.client_id = client_id
        self.msg_type = msg_type
        self.data = data

    def __str__(self):
        return 'ProtocolMessage(host={1},client_id={2},data={3}'.format(
            self.host, self.client_id, self.data
        )


class Worker(object, metaclass=ABCMeta):
    @abstractmethod
    async def on_request(self, request):
        """
        http请求
        """
        pass

    @abstractmethod
    async def on_schedule(self, name, args):
        """
        定时回调，响应yueban3.worker.schedule_once的调用
        """
        pass

    @abstractmethod
    async def on_proto(self, message):
        """
        message是ProtocolMessage对象
        """
        pass

    @abstractmethod
    async def on_client_closed(self, client_id):
        """
        客户端主动或异常断开连接，master会做清理，不必再通知master
        """
        pass


async def _yueban_handler(request):
    path = request.path
    try:
        if path == communicate.WorkerPath.Proto:
            msg = await utility.unpack_pickle_request(request)
            client_id = msg["id"]
            msg_type = msg["type"]
            data = msg["data"]
            host = msg["host"]
            msg_obj = ProtocolMessage(host, client_id, msg_type, data)
            await _worker_app.on_proto(msg_obj)
            return utility.pack_pickle_response('')
        elif path == communicate.WorkerPath.ClientClosed:
            msg = await utility.unpack_pickle_request(request)
            client_id = msg["id"]
            await _worker_app.on_client_closed(client_id)
            return utility.pack_pickle_response('')
        elif path == communicate.WorkerPath.OnSchedule:
            msg = await utility.unpack_pickle_request(request)
            name = msg["name"]
            args = msg["args"]
            await _worker_app.on_schedule(name, args)
            return utility.pack_pickle_response('')
        elif path == communicate.WorkerPath.ProxyReloadConfig:
            rets = await communicate.call_all_masters(communicate.MasterPath.ReloadConfig, {})
            return utility.pack_pickle_response(rets)
        else:
            return await _worker_app.on_request(request)
    except Exception as e:
        bs = await request.read()
        ts = traceback.format_exc()
        log.error("yueban_hander", path, bs, e, ts)


async def unicast(client_id, data):
    """
    单播
    """
    path = communicate.MasterPath.Proto
    master_id = communicate.get_master_id(client_id)
    msg = {
        "ids": [client_id],
        "data": data,
    }
    await communicate.call_specific_master(master_id, path, msg)


async def multicast(client_ids, data):
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
        msg = {
            "ids": mids,
            "data": data,
        }
        task = communicate.call_specific_master(master_id, path, msg)
        tasks.append(task)
    await asyncio.wait(tasks)


async def broadcast(data):
    """
    广播
    """
    path = communicate.MasterPath.Proto
    msg = {
        "ids": [],
        "data": data,
    }
    await communicate.call_all_masters(path, msg)


async def close_client(client_id):
    """
    断开一个客户端
    """
    path = communicate.MasterPath.CloseClient
    master_id = communicate.get_master_id(client_id)
    msg = {
        "id": client_id,
    }
    await communicate.call_specific_master(master_id, path, msg)


async def schedule_once(seconds, name, args):
    """
    延迟回调
    """
    path = communicate.MasterPath.Schedule
    msg = {
        "duration": seconds,
        "name": name,
        "args": args,
    }
    await communicate.call_master(path, msg)


def get_web_app():
    return _web_app


def get_worker_app():
    return _worker_app


async def _on_shutdown():
    if _grace_timeout <= 0:
        return
    await asyncio.sleep(_grace_timeout)
    await communicate.cleanup()
    await cache.cleanup()
    await storage.cleanup()


async def initialize(cfg_path, worker_app, grace_timeout=5):
    global _worker_app
    global _web_app
    global _grace_timeout
    if not isinstance(worker_app, Worker):
        raise TypeError("bad worker instance type")
    _grace_timeout = grace_timeout
    _worker_app = worker_app
    configuration.init(cfg_path)
    await log.initialize()
    await communicate.initialize()
    await table.initialize()
    tasks = [
        cache.initialize(),
        storage.initialize(),
    ]
    await asyncio.gather(*tasks)
    _web_app = web.Application()
    _web_app.router.add_get("/{path:.*}", _yueban_handler)
    _web_app.router.add_post("/{path:.*}", _yueban_handler)
    _web_app.on_shutdown.append(_on_shutdown)
    return _web_app
