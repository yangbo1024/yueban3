# -*- coding:utf-8 -*-

"""
Master
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


_c2s_heart_beat = globals().setdefault("_c2s_heart_beat", "_hb")
_s2c_heart_beat = globals().setdefault("_s2c_heart_beat", "_hb")
_max_idle_time = globals().setdefault("_max_idle_time", 60)
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
    global _gate_id
    log.info(_gate_id, *args)


def log_error(*args):
    global _gate_id
    log.error(_gate_id, *args)


def set_heartbeat_proto_id(c2s_id, s2c_id):
    global _c2s_heart_beat
    global _s2c_heart_beat
    _c2s_heart_beat = c2s_id
    _s2c_heart_beat = s2c_id
    log_info("set_heartbeat_proto", c2s_id, s2c_id)


def set_max_idle_time(sec):
    global _max_idle_time
    _max_idle_time = sec


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
        log_error("remove_client_error", e, s)
    _clients.pop(client_id)
    return True


async def _send_routine(client_obj, ws):
    client_id = client_obj.client_id
    queue = client_obj.send_queue
    while 1:
        try:
            msg = await queue.get()
            if msg is None:
                # only in _remove_client can be None
                log_info('send_queue_none', client_id)
                break
            await ws.send_str(msg)
        except Exception as e:
            remove_client(client_id)
            log_error('send_routine_error', client_id, e, traceback.format_exc())
            break


async def _recv_routine(client_obj, ws):
    """
    接收处理客户端协议
    """
    client_id = client_obj.client_id
    while 1:
        try:
            msg = await ws.receive(timeout=_max_idle_time)
            if msg.type == web.WSMsgType.TEXT:
                msg_object = json.loads(msg.data)
                proto_id = msg_object["id"]
                echo = msg_object.get("echo", "")
                if proto_id == _s2c_heart_beat:
                    # 心跳协议直接在网关处理
                    q = client_obj.send_queue
                    msg_reply = {
                        "id": _s2c_heart_beat,
                        "body": {},
                        "time": time.time(),
                        "echo": echo,
                    }
                    reply_text = json.dumps(msg_reply)
                    await q.put(reply_text)
                else:
                    body = msg_object["body"]
                    await communicate.call_worker(communicate.WorkerPath.Proto, [_gate_id, client_id, proto_id, body])
            else:
                remove_client(client_id)
                await communicate.call_worker(communicate.WorkerPath.ClientClosed, [_gate_id, client_id])
                break
        except Exception as e:
            remove_client(client_id)
            log_error('recv_routine_error', client_id, e, traceback.format_exc())
            await communicate.call_worker(communicate.WorkerPath.ClientClosed, [_gate_id, client_id])
            break


async def _websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    client_host = request.headers.get("X-Real-IP") or request.remote
    client_id = gen_client_id()
    client_obj = _add_client(client_id, client_host)
    client_obj.send_queue = asyncio.Queue(128)
    send_task = asyncio.ensure_future(_send_routine(client_obj, ws))
    recv_task = asyncio.ensure_future(_recv_routine(client_obj, ws))
    client_obj.send_task = send_task
    client_obj.recv_task = recv_task
    log_info('begin', client_id, client_host, len(_clients))
    await asyncio.wait([send_task, recv_task], return_when=asyncio.FIRST_COMPLETED)
    log_info("end", client_id, len(_clients))
    return ws


# server to gate
async def _proto_handler(request):
    bs = await request.read()
    data = communicate.loads(bs)
    client_ids, proto_id, body = data
    if not client_ids:
        # broadcast
        client_ids = _clients.keys()
    try:
        msg_object = {
            "id": proto_id,
            "body": body,
            "time": time.time(),
        }
        msg = json.dumps(msg_object)
    except Exception as e:
        s = traceback.format_exc()
        log_error("proto_dump_error", client_ids, proto_id, body, e, s)
        return
    for client_id in client_ids:
        client_obj = _clients.get(client_id)
        if not client_obj:
            continue
        try:
            q = client_obj.send_queue
            q.put_nowait(msg)            
        except Exception as e:
            s = traceback.format_exc()
            log_error("proto_handler_error", client_id, proto_id, body, e, s)
    return utility.pack_pickle_response('')


async def _close_client_handler(request):
    bs = await request.read()
    data = communicate.loads(bs)
    client_ids = data
    for client_id in client_ids:
        remove_client(client_id)
    return utility.pack_pickle_response('')


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
        await communicate.call_worker(path, [name, args])
    except Exception as e:
        import traceback
        await log_error('schedule_error', name, args, e, traceback.format_exc())
    finally:
        _schedule_cnt -= 1


async def _schedule_handler(request):
    bs = await request.read()
    msg = communicate.loads(bs)
    seconds, name, args = msg
    asyncio.ensure_future(_future(seconds, name, args))
    return utility.pack_pickle_response('')


def get_gate_id():
    return _gate_id


def set_gate_id(gate_id):
    global _gate_id
    _gate_id = gate_id


_handlers = {
    communicate.MasterPath.Proto: _proto_handler,
    communicate.MasterPath.CloseClient: _close_client_handler,
    communicate.MasterPath.Hotfix: _hotfix_handler,
    communicate.MasterPath.Schedule: _schedule_handler,
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
