# -*- coding:utf-8 -*-

"""
excel->json
数据表处理
每个表第一行为表头，第二行起为数据
所有数据已经转换为表头中的类型
"""

import json
from . import configuration
import os
import os.path
import copy


_cached_mtimes = {}
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
    path = _get_table_path(table_name)
    stat_info = os.stat(path)
    old_time = _cached_mtimes.get(table_name, 0)
    try:
        if stat_info.st_mtime > old_time:
            _cached_mtimes[table_name] = stat_info.st_mtime
            # update table data
            table_data = _load_table_data(path)
            _cached_tables[table_name] = table_data
            return table_data
        else:
            table_data = _cached_tables.get(table_name)
            if not table_data:
                # first time load
                table_data = _load_table_data(path)
                _cached_tables[table_name] = table_data
            return table_data
    except Exception as e:
        from . import utility
        import traceback
        s = traceback.format_exc()
        utility.print_out("get_newest_table_error", table_name, e, s)
        raise e


def update_table(table_name, table_data_str, encoding='utf-8'):
    """
    更新表，主要给系统接口用
    :param table_name:
    :param table_data_str:
    :param encoding:
    :return:
    """
    path = _get_table_path(table_name)
    with open(path, 'w', encoding=encoding) as f:
        f.write(table_data_str)
    _get_newest_table_data(table_name)


def get_table(table_name, clone=True):
    """
    获取整个表数据
    :param table_name:
    :param clone:
    :return:
    """
    data = _get_newest_table_data(table_name)
    return copy.deepcopy(data) if clone else data


def get_rows(table_name, index_name, index_value, clone=True):
    """
    获取能够匹配的所有行
    :param table_name:
    :param index_name:
    :param index_value:
    :param clone:
    :return:
    """
    table_data = _get_newest_table_data(table_name)
    if not table_data:
        return None
    ret = [row_data for row_data in table_data if row_data[index_name] == index_value]
    return copy.deepcopy(ret) if clone else ret


def get_row(table_name, index_name, index_value, clone=True):
    """
    获取1行
    :param table_name:
    :param index_name:
    :param index_value:
    :param clone:
    :return:
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
    :param table_name:
    :param index_name:
    :param index_value:
    :param query_column:
    :param clone:
    :return:
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

