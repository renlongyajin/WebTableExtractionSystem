# -*- coding: utf-8 -*-
import os
from collections import OrderedDict
from copy import deepcopy


def init():  # 初始化
    global __global_dict
    __global_dict = OrderedDict()


def set_value(key: str, value) -> None:
    """ 定义一个全局变量 """
    __global_dict[key] = value


def get_value(key: str, defValue=None):
    """ 获得一个全局变量,不存在则返回默认值 """
    try:
        return __global_dict[key]
    except KeyError:
        print(f"该项目中不存在全局变量:{key}")


def getGlobalDictCopy() -> dict:
    """
    获得当前全局变量的复制字典
    :return:
    """
    return deepcopy(__global_dict)
