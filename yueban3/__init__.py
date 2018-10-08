# -*- coding:utf-8 -*-
"""
框架:
    基于aiohttp，aioredis, motor

配置：
    参见configuration.py配置模板

特点:
    无状态
    一个redis做cache
    一个mongodb做数据存储
    日志按天切割

服务一共分为以下几类：
    1. master
        网关，维持客户端websocket长连，定时回调
    2. worker
        逻辑功能，可随时热更
"""

import asyncio
from . import configuration
from . import log
from . import cache
from . import storage


__version__ = "1.0.0"


async def init_worker(cfg):
    configuration.set_config(cfg)
    await log.initialize()
    tasks = [
        cache.initialize(),
        storage.initialize(),
    ]
    await asyncio.wait(tasks)
