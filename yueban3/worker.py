# -*- coding:utf-8 -*-
"""
worker-逻辑进程
通过gunicorn启动，
可以热更新
"""

import asyncio
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
import sanic
from sanic import response
import socket


_web_app = sanic.Sanic()
_worker_app = None
_cfg_path = ''
_worker_id = ""


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
        return 'ProtocolMessage(host={0},client_id={1},data={2}'.format(
            self.host, self.client_id, self.data
        )


class Worker(object, metaclass=ABCMeta):
    def __init__(self, worker_id):
        self.worker_id = worker_id

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
        如果返回对象不为None，则作为应答数据返回给客户端
        """
        pass

    @abstractmethod
    async def on_client_closed(self, client_id):
        """
        客户端主动或异常断开连接，master会做清理，不必再通知master
        """
        pass


@_web_app.route('/__/<name>', methods=['GET', 'POST'])
async def _yueban_handler(request, name):
    path = request.path
    try:
        if path == communicate.WorkerPath.Proto:
            msg = await utility.unpack_pickle_request(request)
            client_id = msg["id"]
            msg_type = msg["type"]
            data = msg["data"]
            host = msg["host"]
            msg_obj = ProtocolMessage(host, client_id, msg_type, data)
            data = await _worker_app.on_proto(msg_obj)
            # data不为None表示有应答数据，一应一答的场景下可以省掉调用master的开销
            return utility.pack_pickle_response(data)
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
            log.error('bad_yueban_handler', path, request.ip)
            return response.text('bad request')
    except Exception as e:
        bs = request.body
        ts = traceback.format_exc()
        log.error("yueban_hander", path, bs, e, ts)
        return utility.pack_pickle_response('')


@_web_app.listener('after_server_stop')
async def _on_shutdown(app, loop):
    log.info('shutdown')
    asyncio.set_event_loop(loop)
    await communicate.cleanup()
    await cache.cleanup()
    await storage.cleanup()
    await log.cleanup()


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
    return await communicate.call_specific_master(master_id, path, msg)


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


@_web_app.listener('before_server_start')
async def _initialize(app, loop):
    global _worker_app
    if not isinstance(_worker_app, Worker):
        raise TypeError("bad worker instance type")
    await log.initialize()
    tasks = [
        communicate.initialize(),
        table.initialize(),
        cache.initialize(),
        storage.initialize(),
    ]
    await asyncio.gather(*tasks)


def run(cfg_path, worker_app, reuse_port=False, settings={}, **kwargs):
    """
    :param cfg_path: 配置文件路径
    :param worker_app: Worker的子类
    :param settings: 配置参数
    :param reuse_port:
    :param settings:
    :param kwargs: 其它需要传递给aiohttp.web.run_app的参数
    :return:
    """
    global _web_app
    global _worker_app
    _worker_app = worker_app
    configuration.init(cfg_path)
    worker_cfg = configuration.get_worker_config()
    cfg = worker_cfg[worker_app.worker_id]
    host = cfg["host"]
    port = cfg["port"]
    _web_app.config.update(settings)
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuse_port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind((host, port))
    sock.set_inheritable(True)
    _web_app.run(sock=sock, **kwargs)

