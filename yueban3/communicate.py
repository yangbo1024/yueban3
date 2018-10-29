# -*- coding:utf-8 -*-

"""
交互、调用
"""

import aiohttp
import asyncio
from . import configuration
import pickle
import random
from . import log


_client_session = None


class MasterPath(object):
    Proto = "/__proto"
    CloseClient = "/__close_client"
    Schedule = "/__schedule"
    Hotfix = "/__hotfix"
    ReloadConfig = "/__reload_config"
    
    
class WorkerPath(object):
    Proto = "/__proto"
    ClientClosed = "/__client_closed"
    OnSchedule = "/__on_schedule"
    ProxyReloadConfig = "/__proxy_reload_config"


def dumps(obj):
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


def loads(bs):
    return pickle.loads(bs)


async def initialize():
    global _client_session
    _client_session = aiohttp.ClientSession()
    return _client_session


def get_client_session():
    """
    请求尽量在一个全局session里发送
    """
    global _client_session
    return _client_session


async def cleanup():
    """
    清理
    """
    global _client_session
    if not _client_session:
        return 
    await _client_session.close()


async def http_get(url, timeout=10):
    """
    HTTP GET请求
    """
    session = _client_session
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError('http_get:{},{}'.format(url, resp.status))
            return resp.status, await resp.read()
    except Exception as e:
        log.error("http_get", e, url)


async def http_post(url, bs, timeout=10):
    """
    HTTP POST请求
    发送：字节流
    接收: 字节流
    """
    session = _client_session
    try:
        async with session.post(url, data=bs, timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError('http_post:{},{},{}'.format(url, bs, resp.status))
            bs = await resp.read()
            return bs
    except Exception as e:
        log.error("http_post", url, bs, e)


async def call_specific_master(master_id, path, args):
    """
    调用指定的Master
    """
    master_config = configuration.get_master_config()
    cfg = master_config[master_id]
    base_url = cfg["url"]
    url = '{0}{1}'.format(base_url, path)
    bs = dumps(args)
    bs = await http_post(url, bs)
    return loads(bs)


async def call_master(path, args):
    """
    随机调用一个Master
    """
    master_config = configuration.get_master_config()
    master_ids = list(master_config.keys())
    master_id = random.choice(master_ids)
    return await call_specific_master(master_id, path, args)


async def call_all_masters(path, args):
    """
    调用所有master
    """
    master_config = configuration.get_master_config()
    tasks = []
    for master_id in master_config:
        task = call_specific_master(master_id, path, args)
        tasks.append(task)
    rets = await asyncio.gather(*tasks)
    return rets


async def call_worker(path, args):
    """
    调用Worker
    """
    worker_config = configuration.get_worker_config()
    worker_ids = list(worker_config.keys())
    worker_id = random.choice(worker_ids)
    cfg = worker_config[worker_id]
    base_url = cfg["url"]
    url = '{0}{1}'.format(base_url, path)
    bs = dumps(args)
    bs = await http_post(url, bs)
    return loads(bs)


def get_master_id(client_id):
    """
    从client_id中解析出master_id
    """
    return client_id.split("_")[0]
