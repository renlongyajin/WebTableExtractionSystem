from datetime import datetime
import traceback
from functools import wraps
import requests.exceptions

# 异常输出
from src.app import gol


def except_output(msg='异常信息'):
    # msg用于自定义函数的提示信息
    def except_execute(func):
        @wraps(func)
        def execept_print(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                sign = '=' * 60 + '\n'
                errorTime = f'{sign}>>>异常时间：\t{datetime.now()}\n>>>异常函数：\t{func.__name__}\n>>>{msg}：\t{e}'
                errorTrace = f'{sign}{traceback.format_exc()}{sign}'
                print(errorTime)
                print(errorTrace)
                with open(f"{gol.get_value('logFilePath')}\\error.log", "a+", encoding='utf-8') as f:
                    f.write(errorTime + "\n")
                    f.write(errorTrace + "\n")

        return execept_print

    return except_execute
