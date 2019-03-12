# -*- coding:utf-8 -*-

"""
Master-不支持重启
可以保持客户端websocket长连接，以及定时回调
"""

import asyncio
from . import utility
from . import communicate
from . import configuration
import traceback
import time
from . import log
from sanic import app
from sanic.app import ConnectionClosed


_web_app = globals().setdefault('_web_app', app.Sanic())
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
    def __init__(self, client_id):
        self.ctime = int(time.time())
        self.client_id = client_id
        self.ip = ''
        self.shut = False       # 是否服务器主动关闭
        self.task = None
        self.ws = None


def _add_client(client_id, ip, ws):
    client_obj = Client(client_id)
    client_obj.ip = ip
    client_obj.ws = ws
    _clients[client_id] = client_obj
    return client_obj


def _pop_client(client_id):
    client_obj = _clients.get(client_id)
    if client_obj:
        _clients.pop(client_id)
        client_obj.ws = None
    return client_obj


async def _c2s(client_obj, args, max_idle):
    # 发协议
    path = communicate.WorkerPath.Proto
    data = await asyncio.shield(communicate.call_worker(path, args))
    if data is not None:
        # data不为None，代表一应一答，类似RPC
        if client_obj.ws:
            await asyncio.wait_for(client_obj.ws.send(data), timeout=max_idle)


async def _serve(client_obj):
    master_config = configuration.get_master_config()
    cfg = master_config[_master_id]
    max_idle = cfg["max_idle"]
    client_id = client_obj.client_id
    ip = client_obj.ip
    ws = client_obj.ws
    while 1:
        try:
            data = await asyncio.wait_for(ws.recv(), timeout=max_idle)
            args = {
                'id': client_id,
                'ip': ip,
                'data': data,
            }
            await _c2s(client_obj, args, max_idle)
        except asyncio.CancelledError:
            break
        except ConnectionClosed:
            break
        except Exception as e:
            s = traceback.format_exc()
            log.info('recv_except', client_id, type(e), s)
            break


@_web_app.websocket('/ws')
async def _websocket_handler(request, ws):
    ip = request.headers.get('X-Real-IP') or request.ip
    client_id = gen_client_id()
    client_obj = _add_client(client_id, ip, ws)
    log.info('begin', client_id, ip, len(_clients))
    task = asyncio.ensure_future(_serve(client_obj))
    client_obj.task = task
    try:
        await task
    finally:
        # 主要是超时或断开
        _pop_client(client_id)
        if not client_obj.shut:
            args = {
                "id": client_id,
                "ip": client_obj.ip,
            }
            path = communicate.WorkerPath.ClientClosed
            await asyncio.shield(communicate.call_worker(path, args))
        log.info('end', client_id, len(_clients))


# worker-logic to master-gate
async def _proto_handler(request):
    msg = await utility.unpack_pickle_request(request)
    client_ids = msg["ids"]
    data = msg["data"]
    if not client_ids:
        # broadcast
        client_ids = _clients.keys()
    ts = []
    for client_id in client_ids:
        client_obj = _clients.get(client_id)
        if not client_obj or not client_obj.ws:
            continue
        t = client_obj.ws.send(data)
        ts.append(t)
    if ts:
        try:
            await asyncio.wait(ts)
        except Exception as e:
            log.error('proto_handler', type(e), len(client_ids), data)
    return utility.pack_pickle_response("")


async def _close_client_handler(request):
    msg = await utility.unpack_pickle_request(request)
    client_id = msg["id"]
    client_obj = _pop_client(client_id)
    log.info('close_client', client_id, bool(client_obj))
    ret = utility.pack_pickle_response('')
    if not client_obj:
        return ret
    client_obj.shut = True
    if client_obj.task and not client_obj.task.done():
        try:
            client_obj.task.cancel()
        except Exception as e:
            log.error('close_cancel', client_id, type(e))
    return ret


async def _hotfix_handler(request):
    ip = request.headers.get('X-Real-IP') or request.ip
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
    log.info('hotfix', ip, result)
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
        log.error('schedule_error', name, args, e, traceback.format_exc())
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
        log.info("reload_config success")
    except Exception as e:
        ret = "fail"
        log.error("reload_config failed", e)
    return utility.pack_json_response(ret)


async def _stat_handler(request):
    online_cnt = len(_clients)
    data = {
        "id": _master_id,
        "online": online_cnt,
        "schedule": _schedule_cnt,
        "incr": _incr_client_id,
    }
    log.info("on_stat")
    return utility.pack_json_response(data)


_handlers = {
    communicate.MasterPath.Proto: _proto_handler,
    communicate.MasterPath.CloseClient: _close_client_handler,
    communicate.MasterPath.Hotfix: _hotfix_handler,
    communicate.MasterPath.Schedule: _schedule_handler,
    communicate.MasterPath.ReloadConfig: _reload_config_handler,
    communicate.MasterPath.Stat: _stat_handler,
}


@_web_app.route('/__/<name>', methods=['GET', 'POST'])
async def _yueban_handler(request, name):
    path = request.path
    handler = _handlers.get(path)
    if not handler:
        log.error('bad_handler', path)
        return utility.pack_json_response(None)
    try:
        ret = await handler(request)
        return ret
    except Exception as e:
        s = traceback.format_exc()
        log.error("yueban_handler", e, s)


@_web_app.listener('after_server_stop')
async def _on_shutdown(app, loop):
    await log.cleanup()


@_web_app.listener('before_server_start')
async def _initialize(app, loop):
    asyncio.set_event_loop(loop)
    await log.initialize()


def run(cfg_path, master_id, settings={'KEEP_ALIVE': False}, **kwargs):
    """
    :param cfg_path: 配置文件路径
    :param master_id: 配置文件中的master服务的id
    :param settings: sanic配置
    :param kwargs: 其它需要传递给aiohttp.web.run_app的参数
    :return:
    """
    global _web_app
    global _master_id
    _master_id = master_id
    configuration.init(cfg_path)
    master_config = configuration.get_master_config()
    cfg = master_config[master_id]
    host = cfg['host']
    port = cfg['port']
    _web_app.config.update(settings)
    _web_app.run(host=host, port=port, **kwargs)

