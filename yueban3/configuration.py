# -*- coding:utf-8 -*-

"""
configuration
"""

import json


_config = {
    "redis": {
        "host": "123.123.123.123",
        "port": 54321,
        "password": "password",
        "db": 0,
        "min_size": 1,
        "max_size": 1,
    },
    "mongodb": {
        "host": "123.123.123.123",
        "port": 54321,
        "password": "password",
        "user": "user",
        "db": "db",
        "replicaset": "",
        "min_pool_size": 1,
        "max_pool_size": 5,
    },
    "master": {
        "m1": {
            "host": "127.0.0.1",
            "port": 10001,
            "url": "http://127.0.0.1:10001",
        }
    },
    "worker": {
        "w1": {
            "host": "127.0.0.1",
            "port": 10102,
            "url": "http://127.0.0.1:10002",
        }
    },
    "table": {
        "dir": "json_table",
    },
    "log": {
        "dir": "logs",
        "name": "log",
    },
}

_config_file_path = "yueban.json"


def reload_config():
    global _config
    with open(_config_file_path) as f:
        bs = f.read()
        _config = json.loads(bs)


def init(path):
    global _config_file_path
    _config_file_path = path
    reload_config()


def set_config(cfg):
    global _config
    _config = cfg


def get_config():
    return _config


def get_redis_config():
    cfg = _config['redis']
    return cfg


def get_mongodb_config():
    cfg = _config['mongodb']
    return cfg


def get_master_config():
    cfg = _config['master']
    return cfg


def get_worker_config():
    cfg = _config['worker']
    return cfg


def get_table_config():
    return _config['table']


def get_log_config():
    return _config['log']


def get_log_dir():
    return _config['log']['dir']


def get_log_name():
    return _config['log']['name']
