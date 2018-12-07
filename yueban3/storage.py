# -*- coding:utf-8 -*-

"""
mongodb访问
"""

from motor import motor_asyncio
from . import configuration
import pymongo
from pymongo import ReadPreference


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
    _client = await create_connection(uri, kwargs)
    # 执行一次验证有没有连接成功
    await _client.admin.command('ismaster')


async def cleanup():
    """
    清理，退出调用
    """
    global _client
    if not _client:
        return None
    _client.close()


def get_client():
    """
    mongodb客户端连接(池)
    """
    return _client


def get_database(read_preference=ReadPreference.PRIMARY):
    """
    获取配置文件中对应的数据库对象
    """
    cfg = configuration.get_mongodb_config()
    db_name = cfg["db"]
    db = _client.get_database(db_name, read_preference=read_preference)
    return db


def get_collection(col_name, read_preference=ReadPreference.PRIMARY):
    """
    获取配置文件中对应的数据库对象中的某个集合
    """
    cfg = configuration.get_mongodb_config()
    db_name = cfg["db"]
    db = _client.get_database(db_name)
    col = db.get_collection(col_name, read_preference=read_preference)
    return col
