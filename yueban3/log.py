# -*- coding:utf-8 -*-

from datetime import datetime
import os
import os.path
import asyncio
from . import configuration


_file = None
_mdt = None
_cache = []
_task = None
_stop = False


def _ensure_log_dir():
    cfg = configuration.get_log_config()
    path = cfg['path']
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


def _ensure_log_file():
    global _file
    global _mdt
    now = datetime.now()
    if _file is None:
        # 创建日志文件
        cfg = configuration.get_log_config()
        path = cfg['path']
        _file = open(path, 'a')
        _mdt = now
        return
    if _mdt.day == now.day:
        return
    cfg = configuration.get_log_config()
    path = cfg['path']
    # 切割日志
    postfix = _mdt.strftime('%Y%m%d')
    dst = '{0}.{1}'.format(path, postfix)
    # 原子操作
    os.rename(path, dst)
    _file.close()
    _file = open(path, 'a')
    _mdt = now


async def _arrange_flush():
    global _cache

    if len(_cache) <= 0:
        # 如果没有写入，也判断一下是否需要切割
        _ensure_log_file()
        return

    log_list = _cache
    _cache = []

    def __flush():
        global _file
        global _mdt
        now = datetime.now()
        _ensure_log_file()
        _mdt = now
        for s in log_list:
            _file.write(s)
        _file.flush()

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, __flush)


async def _loop_flush():
    global _stop
    while not _stop:
        cfg = configuration.get_log_config()
        interval = cfg['interval']
        await asyncio.sleep(interval)
        await _arrange_flush()


async def initialize():
    global _task

    _ensure_log_dir()
    _ensure_log_file()
    _task = asyncio.ensure_future(_loop_flush())


async def cleanup():
    global _stop
    _stop = True
    if not _task.done():
        try:
            await _task
        except Exception as _:
            pass
    await _arrange_flush()


# 自定义log类型
def log(log_type, *args):
    global _cache
    global _stop
    if _stop or not args:
        return
    now = datetime.now()
    time_str = now.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    sl = [time_str, log_type]
    sl.extend([str(arg) for arg in args])
    sl.append(os.linesep)
    s = ' '.join(sl)
    _cache.append(s)


# 写入信息到默认log文件
def info(*args):
    log('INFO', *args)


# 写入错误到默认log文件
def error(*args):
    log('ERROR', *args)
