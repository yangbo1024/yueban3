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


SEND_QUEUE_SIZE = 32


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
    def __init__(self, client_id, ip):
        self.client_id = client_id
        self.ip = ip
        self.send_task = None
        self.recv_task = None
        self.send_queue = None
        self.create_time = int(time.time())
        self.shut = False       # 是否服务器主动关闭
        self.closed = False     # 是否已经被关闭

    def put_s2c(self, msg):
        try:
            self.send_queue.put_nowait(msg)
        except Exception as e:
            log.error('put_s2c', e, self.client_id, msg)


def _add_client(client_id, ip):
    client_obj = Client(client_id, ip)
    _clients[client_id] = client_obj
    return client_obj


def remove_client(client_id, shut=False):
    client_obj = _clients.get(client_id)
    if not client_obj:
        return False
    client_obj.shut = shut
    client_obj.closed = True
    try:
        client_obj.send_queue.put_nowait(None)
    except Exception as e:
        s = traceback.format_exc()
        log.error("remove_client", e, s)
    _clients.pop(client_id)
    return True


async def _send_routine(client_obj, ws):
    master_config = configuration.get_master_config()
    cfg = master_config[_master_id]
    max_idle = cfg["max_idle"]
    client_id = client_obj.client_id
    queue = client_obj.send_queue
    msg = None
    while not client_obj.closed:
        try:
            msg = await queue.get()
            if msg is None:
                # 只有服务器主动断开才会是None，仅仅在remove_client中出现
                # 不直接cancel这个task的目的是为了保证整个数据都发出去再退出，避免发送一半断开的情况
                log.info('sendq_none', client_id)
                break
            # 如果被关闭，发送会抛错
            await asyncio.wait_for(ws.send(msg), timeout=max_idle)
        except (ConnectionClosed, Exception) as e:
            remove_client(client_id)
            if not isinstance(e, ConnectionClosed):
                log.error('send_except', client_id, msg, type(e), traceback.format_exc())
            break


async def _recv_routine(client_obj, ws):
    """
    接收处理客户端协议
    """
    master_config = configuration.get_master_config()
    cfg = master_config[_master_id]
    max_idle = cfg["max_idle"]
    client_id = client_obj.client_id
    while not client_obj.closed:
        try:
            data = await asyncio.wait_for(ws.recv(), timeout=max_idle)
            args = {
                "id": client_id,
                "ip": client_obj.ip,
                "data": data,
            }
            if not client_obj.closed:
                # 没有被关闭才发协议
                data = await communicate.call_worker(communicate.WorkerPath.Proto, args)
                if data is not None:
                    # data不为None，代表一应一答，类似RPC
                    client_obj.put_s2c(data)
        except (ConnectionClosed, Exception) as e:
            # 主要是超时或断开
            remove_client(client_id)
            if not isinstance(e, ConnectionClosed):
                s = traceback.format_exc()
                log.info('recv_except', client_id, type(e), s)
            break


@_web_app.websocket('/ws')
async def _websocket_handler(request, ws):
    ip = request.headers.get('X-Real-IP') or request.ip
    client_id = gen_client_id()
    client_obj = _add_client(client_id, ip)
    try:
        client_obj.send_queue = asyncio.Queue(SEND_QUEUE_SIZE)
        send_task = _send_routine(client_obj, ws)
        recv_task = _recv_routine(client_obj, ws)
        client_obj.send_task = send_task
        client_obj.recv_task = recv_task
        log.info('begin', client_id, ip, len(_clients), _schedule_cnt)
        await asyncio.wait([send_task, recv_task], return_when=asyncio.FIRST_COMPLETED)
    except Exception as e:
        s = traceback.format_exc()
        log.error('ws_except', client_id, type(e), s)
    finally:
        try:
            if not client_obj.shut:
                args = {
                    "id": client_id,
                    "ip": client_obj.ip,
                }
                await communicate.call_worker(communicate.WorkerPath.ClientClosed, args)
        except Exception as e:
            s = traceback.format_exc()
            log.error('close_except', ip, client_id, type(e), s)
        log.info('end', client_id, client_obj.shut)


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
        client_obj.put_s2c(data)
    return utility.pack_pickle_response("")


async def _close_client_handler(request):
    msg = await utility.unpack_pickle_request(request)
    client_id = msg["id"]
    ok = remove_client(client_id, shut=True)
    log.info('close_client', client_id, ok)
    return utility.pack_pickle_response(ok)


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

