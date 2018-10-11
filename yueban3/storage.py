# -*- coding:utf-8 -*-

"""
mongodb访问
"""

from motor import motor_asyncio
from . import configuration
import pymongo


_client = None


async def create_connection(uri, kwargs):
    client = motor_asyncio.AsyncIOMotorClient(uri, **kwargs)
    return client


async def initialize():
    global _client
    cfg = configuration.get_mongodb_config()
    uri = cfg["uri"]
    if not uri:
        return
    kwargs = cfg["args"]
    _client = create_connection(uri, kwargs)
    # 执行一次验证有没有连接成功
    _client.admin.command('ismaster')


async def cleanup():
    global _client
    if not _client:
        return None
    await _client.close()


def get_client():
    return _client


def get_database():
    cfg = configuration.get_mongodb_config()
    db = cfg["db"]
    return _client[db]



