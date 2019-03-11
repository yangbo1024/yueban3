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


class MasterPath(object):
    Proto = "/__/proto"
    CloseClient = "/__/close_client"
    Schedule = "/__/schedule"
    Hotfix = "/__/hotfix"
    ReloadConfig = "/__/reload_config"
    Stat = "/__/stat"
    
    
class WorkerPath(object):
    Proto = "/__/proto"
    ClientClosed = "/__/client_closed"
    OnSchedule = "/__/on_schedule"
    ProxyReloadConfig = "/__/proxy_reload_config"


def dumps(obj):
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


def loads(bs):
    return pickle.loads(bs)


async def post(url, bs, timeout=60):
    """
    POST请求，不用连接池，避免一直发送数据导致因为keep-alive连接不能及时优雅退出
    发送：字节流
    接收: 字节流
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bs, timeout=timeout) as resp:
                if resp.status != 200:
                    raise RuntimeError('')
                bs = await resp.read()
                return bs
    except Exception as e:
        import traceback
        s = traceback.format_exc()
        log.error("http_post", url, bs, e, s)


async def call_specific_master(master_id, path, args):
    """
    调用指定的Master
    """
    master_config = configuration.get_master_config()
    cfg = master_config[master_id]
    base_url = cfg["url"]
    url = '{0}{1}'.format(base_url, path)
    bs = dumps(args)
    bs = await post(url, bs)
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
    bs = await post(url, bs)
    return loads(bs)


def get_master_id(client_id):
    """
    从client_id中解析出master_id
    """
    return client_id.split("_")[0]
