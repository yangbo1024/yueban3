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
import os


_web_app = sanic.Sanic()
_worker_app = None
_cfg_path = ''
_worker_id = ""
_async_tasks = None         # queue
_consumer_task = None


class ProtocolMessage(object):
    """
    长连接协议收到的数据包封装对象
    data可以是二进制也可能是文本
    """
    def __init__(self, ip, client_id, data):
        self.ip = ip
        self.client_id = client_id
        self.data = data

    def __str__(self):
        return 'ProtocolMessage(ip={0},client_id={1},data={2}'.format(
            self.ip, self.client_id, self.data
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
    async def on_client_closed(self, client_id, ip):
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
            data = msg["data"]
            ip = msg["ip"]
            msg_obj = ProtocolMessage(ip, client_id, data)
            data = await _worker_app.on_proto(msg_obj)
            # data不为None表示有应答数据，一应一答的场景下可以省掉调用master的开销
            return utility.pack_pickle_response(data)
        elif path == communicate.WorkerPath.ClientClosed:
            msg = await utility.unpack_pickle_request(request)
            client_id = msg["id"]
            ip = msg['ip']
            await _worker_app.on_client_closed(client_id, ip)
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


# 异步任务
async def _async_consumer():
    while 1:
        t = await _async_tasks.get()
        if t is None:
            break
        try:
            await t
        except Exception as e:
            log.error('_async_consumer', t, e)


@_web_app.listener('before_server_start')
async def _initialize(app, loop):
    global _worker_app
    global _async_tasks
    global _consumer_task
    if not isinstance(_worker_app, Worker):
        raise TypeError("bad worker instance type")
    await log.initialize()
    log.info('start', os.getpid())
    _async_tasks = asyncio.Queue()
    _consumer_task = loop.create_task(_async_consumer())
    tasks = [
        table.initialize(),
        cache.initialize(),
        storage.initialize(),
    ]
    await asyncio.gather(*tasks)


@_web_app.listener('after_server_stop')
async def _on_shutdown(app, loop):
    global _consumer_task
    log.info('stop', os.getpid())
    # 发送停止异步任务信号
    _async_tasks.put_nowait(None)
    if not _consumer_task.done():
        try:
            await _consumer_task
        finally:
            pass
    await cache.cleanup()
    await storage.cleanup()
    await communicate.cleanup()
    # 停止log是最后一步
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
    """
    web框架对象，即sanic对象
    :return:
    """
    global _web_app
    return _web_app


def get_worker_app():
    """
    worker app对象，唯一
    :return:
    """
    global _worker_app
    return _worker_app


def async_execute(coro):
    """
    添加一个异步任务
    :param coro: coroutine
    :return:
    """
    global _async_tasks
    if coro is None:
        return
    _async_tasks.put_nowait(coro)


def run(cfg_path, worker_app, reuse_port=False, settings={'KEEP_ALIVE': False}, **kwargs):
    """
    :param cfg_path: 配置文件路径
    :param worker_app: Worker的子类
    :param settings: 配置参数
    :param reuse_port: 是否允许reuse_port
    :param settings: sanic配置
    :param kwargs: 其它需要传递给sanic.app.run的参数
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
    if reuse_port and not kwargs.get('sock'):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((host, port))
        sock.set_inheritable(True)
        _web_app.run(sock=sock, **kwargs)
    else:
        _web_app.run(host=host, port=port, **kwargs)
