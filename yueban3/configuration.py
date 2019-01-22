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
        "min_pool_size": 1,
        "max_pool_size": 2
    },
    "mongodb": {
        "uri": "mongodb://username:password@host:port/database?replicaSet=test",
        "db": "yueban",
        "args": {
            "minPoolSize": 1,
            "maxPoolSize": 5
        }
    },
    "master": {
        "m1": {
            "host": "0.0.0.0",
            "port": 10100,
            "url": "http://127.0.0.1:10001",
            "heartbeat": 10,
            "max_idle": 600
        }
    },
    "worker": {
        "w1": {
            "host": "0.0.0.0",
            "port": 10200,
            "url": "http://127.0.0.1:10002"
        }
    },
    "table": {
        "dir": "json_table",
        "ext": ".json"
    },
    "log": {
        "interval": 0.2,
        "path": "logs/log.log"
    },
    "custom": {
    }
}

_config_file_path = "yueban.json"


def reload_config():
    global _config
    with open(_config_file_path, encoding="utf-8") as f:
        s = f.read()
        _config = json.loads(s)


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


def get_custom_config():
    return _config['custom']
