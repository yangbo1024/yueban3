# -*- coding:utf-8 -*-

"""
一些常用的封装好的功能、函数等
"""

import random
import uuid
import pickle
import json
import datetime
import os
import os.path
import base64
import aiohttp
from aiohttp import web


def simple_crypt(bs):
    """
    :param bs:
    :return:
    """
    bs = bytearray(bs)
    for i in range(0, len(bs), 3):
        bs[i] ^= 0xab
    return bytes(bs)


def rc4(k, bs):
    """
    :param k: password
    :param bs: the bytes to be encrypted
    :return:
    """
    box = list(range(256))
    j = 0
    klen = len(k)
    for i in range(256):
        j = (j + box[i] + ord(k[i % klen])) % 256
        box[i], box[j] = box[j], box[i]
    out = bytearray(bs)
    i = j = 0
    for pos, ch in enumerate(out):
        i = (i + 1) % 256
        j = (j + box[i]) % 256
        box[i], box[j] = box[j], box[i]
        out[pos] = ch ^ box[(box[i] + box[j]) % 256]
    return bytes(out)


def weight_rand(weights):
    """
    :param weights: list or tuple
    :return:
    """
    sum_weight = sum(weights)
    r = random.uniform(0, sum_weight)
    s = 0
    for i, w in enumerate(weights):
        s += w
        if r < s:
            return i
    return random.choice(range(len(weights)))


def weight_rand_dict(weight_dic):
    """
    Format: {k: weight}
    :param weight_dic:
    :return:
    """
    keys = list(weight_dic.keys())
    weights = [weight_dic[k] for k in keys]
    idx = weight_rand(weights)
    return keys[idx]


def cmp_version(v1, v2):
    """
    Version format: xx.xx.xx
    :param v1:
    :param v2:
    :return: v1<v2, -1;    v2==v2, 0;  v1>v2, 1
    """
    if not v1:
        v1 = '0.0.0'
    if not v2:
        v2 = '0.0.0'
    v1_sp = [int(n) for n in v1.split('.')]
    v2_sp = [int(n) for n in v2.split('.')]
    if v1_sp < v2_sp:
        return -1
    elif v1_sp == v2_sp:
        return 0
    else:
        return 1


def format_time(sec, day_str='天', hour_str='小时', minute_str='分', second_str='秒'):
    """
    日期显示格式化
    :param sec:
    :param day_str:
    :param hour_str:
    :param minute_str:
    :param second_str:
    :return:
    """
    s = ''
    sec = int(sec)
    day, left = divmod(sec, 86400)
    if day > 0:
        s += '{0:d}{1}'.format(day, day_str)
    hour, left = divmod(left, 3600)
    if hour > 0:
        s += '{0:d}{1}'.format(hour, hour_str)
    elif day > 0:
        s += '0{0}'.format(hour_str)
    minute, left = divmod(left, 60)
    if minute > 0:
        s += '{0}{1}'.format(minute, minute_str)
    elif day > 0 or hour > 0:
        s += '0{0}'.format(minute_str)
    s += '{0}{1}'.format(left, second_str)
    return s


def gen_uniq_id(encoding='utf-8'):
    """
    全局唯一ID
    :return: 字符串
    """
    bs = uuid.uuid1().bytes
    uniq_id = base64.urlsafe_b64encode(bs)
    s = str(uniq_id, encoding)
    s = s.replace("=", "")
    return s


async def unpack_pickle_request(request):
    bs = await request.read()
    return pickle.loads(bs)


def pack_pickle_response(data):
    bs = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
    return web.Response(body=bs)


async def unpack_json_request(request):
    bs = await request.read()
    return json.loads(bs)


def pack_json_response(data):
    bs = json.dumps(data)
    return web.Response(body=bs)


def print_out(*args):
    """
    打印到控制台
    """
    s = " ".join([str(arg) for arg in args])
    now = datetime.datetime.now()
    time_str = now.strftime('%Y-%m-%d %H:%M:%S,%f')[:23]
    print(time_str, s)


def ensure_directory(directory):
    """
    确保目录存在
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except FileExistsError:
        pass


def get_local_ips():
    """
    获取本地ip地址
    """
    import socket
    hostname = socket.gethostname()
    addr_infos = socket.getaddrinfo(hostname, 1)
    ips = []
    for addr_info in addr_infos:
        addr = addr_info[4]
        ip = addr[0]
        ips.append(ip)
    return ips

