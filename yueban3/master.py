# -*- coding:utf-8 -*-

"""
Master-不支持重启
可以保持客户端websocket长连接，以及定时回调
"""

from aiohttp import web
import json
import asyncio
from . import utility
from . import communicate
from . import configuration
import traceback
import time
from . import log
import pickle


SEND_QUEUE_SIZE = 128


_web_app = globals().setdefault('_web_app')
_master_id = globals().setdefault('_master_id', '')
_clients = globals().setdefault('_clients', {})
_schedule_cnt = globals().setdefault("_schedule_cnt", 0)
_incr_client_id = globals().setdefault("_incr_client_id", 0)


def gen_client_id():
    global _incr_client_id
    _incr_client_id += 1
    return "{}_{}".format(_master_id, _incr_client_id)


class Client(object):
    """
    客户端对象，与客户端1：1存在
    """
    def __init__(self, client_id, host):
        self.client_id = client_id
        self.host = host
        self.send_task = None
        self.recv_task = None
        self.send_queue = None
        self.create_time = int(time.time())


def log_info(*args):
    global _master_id
    log.info(_master_id, *args)


def log_error(*args):
    global _master_id
    log.error(_master_id, *args)


def _add_client(client_id, host):
    client_obj = Client(client_id, host)
    _clients[client_id] = client_obj
    return client_obj


def remove_client(client_id):
    client_obj = _clients.get(client_id)
    if not client_obj:
        return False
    try:
        client_obj.send_queue.put_nowait(None)
    except Exception as e:
        s = traceback.format_exc()
        log_error("remove_client", e, s)
    _clients.pop(client_id)
    return True


async def _send_routine(client_obj, ws):
    client_id = client_obj.client_id
    queue = client_obj.send_queue
    msg = None
    while 1:
        try:
            msg = await queue.get()
            if msg is None:
                # only in _remove_client can be None
                log_info('sendq_none', client_id)
                break
            if isinstance(msg, str):
                await ws.send_str(msg)
            elif isinstance(msg, bytes):
                await ws.send_bytes(msg)
            else:
                raise TypeError("unknown msg type")
        except Exception as e:
            remove_client(client_id)
            log_error('send_routine', client_id, msg, e, traceback.format_exc())
            break


async def _recv_routine(client_obj, ws):
    """
    接收处理客户端协议
    """
    master_config = configuration.get_master_config()
    cfg = master_config[_master_id]
    max_idle = cfg["max_idle"]
    client_id = client_obj.client_id
    while 1:
        try:
            msg = await ws.receive(timeout=max_idle)
            # auto ping&pong, 所以只有消息包会收到
            if msg.type in (web.WSMsgType.TEXT, web.WSMsgType.BINARY):
                args = {
                    "id": client_id,
                    "host": client_obj.host,
                    "type": msg.type,
                    "data": msg.data,
                }
                await communicate.call_worker(communicate.WorkerPath.Proto, args)
            elif msg.type in (web.WSMsgType.PING, web.WSMsgType.PONG):
                # 兼容框架
                continue
            else:
                remove_client(client_id)
                args = {
                    "id": client_id,
                    "host": client_obj.host,
                }
                await communicate.call_worker(communicate.WorkerPath.ClientClosed, args)
                break
        except Exception as e:
            remove_client(client_id)
            log_error('recv_routine', client_id, e, traceback.format_exc())
            args = {
                "id": client_id,
                "host": client_obj.host,
            }
            await communicate.call_worker(communicate.WorkerPath.ClientClosed, args)
            break


async def _websocket_handler(request):
    master_config = configuration.get_master_config()
    cfg = master_config[_master_id]
    _heartbeat = cfg["heartbeat"]
    if _heartbeat <= 0:
        ws = web.WebSocketResponse()
    else:
        ws = web.WebSocketResponse(heartbeat=_heartbeat)
    await ws.prepare(request)
    client_host = request.headers.get("X-Real-IP") or request.remote
    client_id = gen_client_id()
    client_obj = _add_client(client_id, client_host)
    client_obj.send_queue = asyncio.Queue(SEND_QUEUE_SIZE)
    send_task = asyncio.ensure_future(_send_routine(client_obj, ws))
    recv_task = asyncio.ensure_future(_recv_routine(client_obj, ws))
    client_obj.send_task = send_task
    client_obj.recv_task = recv_task
    log_info('begin', client_id, client_host, len(_clients), _schedule_cnt)
    await asyncio.wait([send_task, recv_task], return_when=asyncio.FIRST_COMPLETED)
    log_info("end", client_id, len(_clients), _schedule_cnt)
    try:
        await ws.close()
    except asyncio.CancelledError:
        log_error("ws cancelled", client_id)
    else:
        pass
    return ws


# worker-logic to master-gate
async def _proto_handler(request):
    msg = await utility.unpack_pickle_request(request)
    client_ids = msg["ids"]
    data = msg["data"]
    if not client_ids:
        # broadcast
        client_ids = _clients.keys()
    for client_id in client_ids:
        client_obj = _clients.get(client_id)
        if not client_obj:
            continue
        try:
            q = client_obj.send_queue
            q.put_nowait(data)            
        except Exception as e:
            s = traceback.format_exc()
            log_error("proto_handler", client_id, data, e, s)
    return utility.pack_pickle_response("")


async def _close_client_handler(request):
    msg = await utility.unpack_pickle_request(request)
    client_id = msg["id"]
    ok = remove_client(client_id)
    return utility.pack_pickle_response(ok)


async def _hotfix_handler(request):
    host = request.headers.get("X-Real-IP") or request.remote
    import importlib
    try:
        importlib.invalidate_caches()
        m = importlib.import_module('hotfix')
        importlib.reload(m)
        result = m.run()
    except Exception as e:
        import traceback
        result = [e, traceback.format_exc()]
    result = str(result)
    log_info('hotfix', host, result)
    return utility.pack_json_response(result)


async def _future(seconds, name, args):
    global _schedule_cnt
    _schedule_cnt += 1
    await asyncio.sleep(seconds)
    try:
        path = communicate.WorkerPath.OnSchedule
        msg = {
            "name": name,
            "args": args,
        }
        await communicate.call_worker(path, msg)
    except Exception as e:
        import traceback
        await log_error('schedule_error', name, args, e, traceback.format_exc())
    finally:
        _schedule_cnt -= 1


async def _schedule_handler(request):
    msg = await utility.unpack_pickle_request(request)
    duration = msg["duration"]
    name = msg["name"]
    args = msg["args"]
    asyncio.ensure_future(_future(duration, name, args))
    return utility.pack_pickle_response(0)


async def _reload_config_handler(request):
    try:
        configuration.reload_config()
        ret = "success"
    except:
        ret = "fail"
    return utility.pack_json_response(ret)


_handlers = {
    communicate.MasterPath.Proto: _proto_handler,
    communicate.MasterPath.CloseClient: _close_client_handler,
    communicate.MasterPath.Hotfix: _hotfix_handler,
    communicate.MasterPath.Schedule: _schedule_handler,
    communicate.MasterPath.ReloadConfig: _reload_config_handler,
}


async def _yueban_handler(request):
    handler = _handlers.get(request.path)
    if not handler:
        log_error('bad handler', request.path)
        return utility.pack_json_response(None)
    try:
        ret = await handler(request)
        return ret
    except Exception as e:
        s = traceback.format_exc()
        log_error("error", e, s)


async def initialize(cfg_path):
    configuration.init(cfg_path)
    await log.initialize()
    await communicate.initialize()
    # web
    _web_app = web.Application()
    _web_app.router.add_get("/__ws", _websocket_handler)
    _web_app.router.add_post('/{path:.*}', _yueban_handler)


def run(cfg_path, master_id):
    global _web_app
    global _master_id
    _master_id = master_id
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize(cfg_path))
    master_config = configuration.get_master_config()
    cfg = master_config[master_id]
    host = cfg['host']
    port = cfg['port']
    web.run_app(_web_app, host=host, port=port, access_log=None)
