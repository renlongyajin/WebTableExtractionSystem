#!/user/bin/env python3
# -*- coding: utf-8 -*-

class TableFeatures:
    """表格特征"""

    def __init__(self):
        self.averageColNum = 0  # 平均列数
        self.averageRowNum = 0  # 平均行数
        self.colNumStandardDeviation = 0  # 列数标准偏差
        self.rowNumStandardDeviation = 0  # 行数标准偏差
        self.averageItemLength = 0  # 平均单元格长度
        self.itemLengthStandardDeviation = 0  # 单元格长度标准偏差
