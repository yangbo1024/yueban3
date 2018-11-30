# -*- coding:utf-8 -*-

"""
redis访问
"""

import aioredis
from . import configuration
from . import log

_redis_pool = None


# 以SYS_KEY_PREFIX开头的key，是系统保留key
SYS_KEY_PREFIX = '__'


def make_key(*fields):
    return ':'.join(fields)


async def create_pool(host, port, password, db, minsize, maxsize):
    return await aioredis.create_redis_pool((host, port), db=db, password=password, minsize=minsize, maxsize=maxsize)


async def initialize():
    global _redis_pool
    cfg = configuration.get_redis_config()
    host = cfg['host']
    port = cfg['port']
    password = cfg['password']
    db = cfg['db']
    minsize = cfg['min_pool_size']
    maxsize = cfg['max_pool_size']
    _redis_pool = await create_pool(host, port, password, db, minsize, maxsize)


async def cleanup():
    global _redis_pool
    if not _redis_pool:
        return
    _redis_pool.close()
    await _redis_pool.wait_closed()


def get_connection_pool():
    return _redis_pool


class Lock(object):
    """
    效率低，少用
    注意避免递归锁
    为防止忘记关闭锁的情况，暂时不提供
        begin_lock
        end_lock
    的调用方式
    usage:
        async with Lock(lock_resource_name, timeout_seconds) as lock:
            if not lock:
                error_handle
            else:
                do_some_thing()
                print(lock.lock_id)
    """
    UNLOCK_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[2] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    # 可以根据需求启用lua
    lua_valid = True
    def __init__(self, lock_name, timeout=5.0, retry_interval=0.01, lua_valid=None):
        """
        开启lua可以在解锁时只请求1次；否则请求2次；
        因为主流云服务的redis对lua支持不好，所以注意选择是否禁用lua，避免调用失败
        """
        from . import utility
        self.lock_key = make_key(SYS_KEY_PREFIX, lock_name)
        self.lock_id = utility.gen_uniq_id()
        self.timeout = max(0.02, timeout)
        self.interval = max(0.001, retry_interval)
        if lua_valid is not None:
            self.lua_valid = lua_valid

    async def __aenter__(self):
        import asyncio
        nx = _redis_pool.SET_IF_NOT_EXIST
        p_timeout = int(self.timeout * 1000)
        p_interval = int(self.interval * 1000)
        p_sum_time = 0
        while p_sum_time < p_timeout:
            ok = await _redis_pool.set(self.lock_key, self.lock_id, pexpire=p_interval, exist=nx)
            if not ok:
                await asyncio.sleep(self.interval)
                p_sum_time += p_interval
                continue
            return self
        msg = "lock failed:{} {} {}".format(self.lock_key, self.lock_id, self.timeout)
        log.error(msg)
        raise RuntimeError(msg)

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            import traceback
            el = traceback.format_exception(exc_type, exc, tb)
            es = "".join(el)
            log.error('lock_exc_error:\n', self.lock_key, es)
        if self.lua_valid:
            await _redis_pool.eval(self.UNLOCK_SCRIPT, keys=[self.lock_key], args=[self.lock_id])
        else:
            locked_id = await _redis_pool.get(self.lock_key)
            if locked_id == self.lock_id:
                await _redis_pool.delete(self.lock_key)
