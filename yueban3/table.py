# -*- coding:utf-8 -*-

"""
excel->json
数据表处理
每个表第一行为表头，第二行起为数据
所有数据已经转换为表头中的类型
"""

import json
from . import configuration
import os.path
import copy
from . import log


_cached_tables = {}


def _get_table_path(table_name):
    table_config = configuration.get_table_config()
    table_data_dir = table_config["dir"]
    ext = table_config["ext"]
    table_file_name = table_name + ext
    path = os.path.join(table_data_dir, table_file_name)
    return path


def _load_table_data(path):
    table_data = []
    with open(path) as f:
        data_str = f.read()
        data = json.loads(data_str)
        headers = []
        for i, row in enumerate(data):
            if i == 0:
                for header in row:
                    headers.append(header)
            else:
                if row[0] is None:
                    break
                row_data = {}
                for j, col in enumerate(row):
                    row_data[headers[j]] = col
                table_data.append(row_data)
    return table_data


def _get_newest_table_data(table_name):
    table_data = _cached_tables.get(table_name)
    if table_data:
        return table_data
    path = _get_table_path(table_name)
    table_data = _load_table_data(path)
    _cached_tables[table_name] = table_data
    return table_data


def update_table(table_name):
    """
    更新表，主要给系统接口用
    :param table_name:
    :return:
    """
    if table_name in _cached_tables:
        _cached_tables.pop(table_name)


def get_table(table_name, clone=True):
    """
    获取整个表数据
    """
    data = _get_newest_table_data(table_name)
    return copy.deepcopy(data) if clone else data


def get_rows(table_name, index_name, index_value, clone=True):
    """
    获取能够匹配的所有行
    """
    table_data = _get_newest_table_data(table_name)
    if not table_data:
        return None
    ret = [row_data for row_data in table_data if row_data[index_name] == index_value]
    return copy.deepcopy(ret) if clone else ret


def get_row(table_name, index_name, index_value, clone=True):
    """
    获取1行
    """
    table_data = _get_newest_table_data(table_name)
    if not table_data:
        return None
    for row_data in table_data:
        if row_data[index_name] == index_value:
            return copy.deepcopy(row_data) if clone else row_data
    return None


def get_cell(table_name, index_name, index_value, query_column, clone=True):
    """
    获取一个格子的内容
    """
    row_map = get_row(table_name, index_name, index_value)
    if not row_map:
        return None
    cell = row_map.get(query_column)
    return copy.deepcopy(cell) if clone else cell


async def initialize():
    from . import utility
    table_config = configuration.get_table_config()
    table_data_dir = table_config["dir"]
    if not table_data_dir:
        return
    utility.ensure_directory(table_data_dir)

