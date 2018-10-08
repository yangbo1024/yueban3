# -*- coding:utf-8 -*-

"""
mongodb访问
"""

from motor import motor_asyncio
from . import configuration
import pymongo


_db = None


async def create_connection(host, port, database, user, password, replicaset='', min_pool_size=1, max_pool_size=5):
    client = motor_asyncio.AsyncIOMotorClient(host, port, replicaset=replicaset, minPoolSize=min_pool_size, maxPoolSize=max_pool_size)
    db = client[database]
    await db.authenticate(user, password)
    return db


async def create_connection_of_config():
    cfg = configuration.get_mongodb_config()
    host = cfg['host']
    if not host:
        return None
    port = cfg['port']
    password = cfg['password']
    user = cfg['user']
    db = cfg['db']
    replicaset = cfg['replicaset']
    min_pool_size = cfg['min_pool_size']
    max_pool_size = cfg['max_pool_size']
    return await create_connection(host, port, db, user, password, replicaset, min_pool_size, max_pool_size)


async def initialize():
    global _db
    _db = await create_connection_of_config()


def get_database():
    return _db



