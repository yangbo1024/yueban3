# -*- coding:utf-8 -*-

"""
内部交互、调用
"""

import aiohttp
import asyncio
from . import configuration
import pickle
import random


_client_session = None


class MasterPath(object):
    Proto = "/__proto"
    CloseClient = "/__close_client"
    Schedule = "/__schedule"
    Hotfix = "/__hotfix"
    
    
class WorkerPath(object):
    Proto = "/__proto"
    ClientClosed = "/__client_closed"
    OnSchedule = "/__on_schedule"


def dumps(obj):
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


def loads(bs):
    return pickle.loads(bs)


async def get_client_session():
    global _client_session
    _client_session = aiohttp.ClientSession()


async def cleanup():
    """
    清理
    """
    if not _client_session:
        return 
    await _client_session.close()


async def http_get(url):
    """
    HTTP GET请求
    """
    async with _client_session.get(url) as resp:
        if resp.status != 200:
            raise RuntimeError('http_get:{},{}'.format(url, resp.status))
        return resp.status, await resp.read()


async def http_post(url, bs):
    """
    HTTP POST请求
    发送：字节流
    接收: 字节流
    """
    async with _client_session.post(url, data=bs) as resp:
        if resp.status != 200:
            raise RuntimeError('http_post:{},{},{}'.format(url, bs, resp.status))
        bs = await resp.read()
        return bs


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
