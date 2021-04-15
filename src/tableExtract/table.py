import json
import os
from copy import deepcopy

from bs4 import Tag
from bs4 import NavigableString
import numpy as np
import re
from typing import Tuple

from docx import Document
from pyhanlp import HanLP
from treelib import Tree
from docx.oxml.ns import qn
from docx.shared import Pt
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.enum.table import WD_TABLE_ALIGNMENT

from src.IO.fileInteraction.FileIO import FileIO
from src.app import gol
from src.tools.algorithm.exceptionCatch import except_output


class TableItem:
    """
    表格单元类
    """

    def __init__(self, content, rowLoc, rowspan, colLoc, colspan,
                 href=None, imgSrc=None, type_=None, tagName=None):
        if href is None:
            href = {}
        if imgSrc is None:
            imgSrc = []
        self.content = content  # 表格单元内容
        self.rowLoc = rowLoc  # 表格单元的行位置
        self.rowspan = rowspan  # 表格单元的行占格
        self.colLoc = colLoc  # 表格单元的列位置
        self.colspan = colspan  # 表格单元的列占格
        self.absoluteRow = self.rowLoc  # 表格单元绝对行位置
        self.absoluteCol = self.colLoc  # 表格二单元的绝对列位置
        self.href = href  # 表格单元中含有的链接
        self.img = imgSrc  # 表格单元的图像Src
        self.type_ = type_  # 表格单元的类型
        self.wordType = None  # 表格单词类型
        self.tagName = tagName  # 表格单元的标签名

    def getTableItemType(self) -> str:
        """
        求得表格单元的类型
        :return: 返回类型值
        """
        if self.type_:
            return self.type_
        # TODO:将返回的字符串变成枚举类型
        typeSymbol = re.compile(r"^[\W]*$")  # 匹配符号类型
        typeNumber = re.compile(r"^([\$\uFFE5]?)(-?)(\d+)(\.\d+)?([\u4e00-\u9fa5\%]?)$")  # 匹配数字类型
        typeNumLess0 = re.compile(r"^((-\d+(\.\d+)?)|(0+(\.0+)?))$")  # 小于等于0的数字范围
        typeNum0_1 = re.compile(r"^0(\.\d+)?$")  # 0-1的数字范围
        typeNumGreater1 = re.compile(r"^(([1-9]\d+)|[1-9])(\.[\d]*)?$")  # 大于1的数字范围
        typeChinese = re.compile(r"[\u4e00-\u9fa5]+$")  # 匹配纯汉字
        typeEnglish = re.compile(r"[A-Za-z]+$")  # 匹配英语
        typeEngLowCase = re.compile(r"[a-z]+$")  # 匹配英语小写
        typeEngUpperCase = re.compile(r"[A-Z]+$")  # 匹配英语大写
        typeCharacterAndNum = re.compile(r"[\u4e00-\u9fa5A-Za-z0-9]+$")  # 字符数字类型表达式
        typeHypeLink = re.compile(r"(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")
        content = str(self.content).strip()
        if len(self.img) > 0:
            self.type_ = "图片"
        elif re.match(typeHypeLink, content):
            self.type_ = "超链接"
        elif re.match(typeSymbol, content):
            self.type_ = "标点类型"
        elif re.match(typeCharacterAndNum, content):
            if re.match(typeNumber, content):  # 数字类型
                if re.match(typeNumLess0, content):
                    self.type_ = "<=0"
                elif re.match(typeNum0_1, content):
                    self.type_ = "0-1"
                elif re.match(typeNumGreater1, content):
                    self.type_ = ">=1"
                else:
                    self.type_ = "数字类型"
            else:  # 字符类型
                if re.match(typeChinese, content):
                    self.type_ = "中文"
                elif re.match(typeEnglish, content):
                    if re.match(typeEngUpperCase, content):
                        self.type_ = "大写"
                    elif re.match(typeEngLowCase, content):
                        self.type_ = "小写"
                    else:
                        self.type_ = "大小写混合"
                else:
                    self.type_ = "字符类型"
        else:
            self.type_ = "其他类型"
        return self.type_

    def getTableItemWordType(self) -> str:
        otherConfigurationPath = gol.get_value("otherConfigurationPath")
        if self.wordType:
            return self.wordType
        segment = HanLP.newSegment()
        segment.enableNameRecognize(True)
        result = list(segment.seg(str(self.content)))
        typeList = [str(pair.nature) for pair in result]
        wordDict = FileIO.readPkl(f"{otherConfigurationPath}\\wordMap.pkl")
        numSum = 0
        for type_ in typeList:
            numSum += wordDict[type_]
        typeString = "".join(typeList)
        self.wordType = typeString
        return self.wordType


class Table:
    """
    表格类
    """

    def __init__(self, rowNumber: int = 0, colNumber: int = 0, name: str = "未命名表格",
                 table=None, unfoldDirection=None):
        self.rowNumber = rowNumber  # 表格的行数
        self.colNumber = colNumber  # 表格的列数
        if table is None:  # 表格所在的二维数组
            self.Table = [[TableItem(content=0, rowLoc=j, rowspan=1, colLoc=i, colspan=1)
                           for i in range(self.colNumber)]
                          for j in range(self.rowNumber)]
        else:
            self.Table = table
        self.name = name  # 表格的名称
        self.prefix = None  # 表格的前驱
        self.unfoldDirection = unfoldDirection  # 表格的展开方向
        self.__isCorrect = True  # 当前表格是否正确，行列单元数相同
        self.__isNormal = True  # 当前表格是否正常，行列数均大于等于2
        self.propertyList = []  # 属性单元列表
        self.propertyNameList = []  # 属性名列表
        self.propertyLineNum = 1  # 属性行数
        self.tableType = None  # 表格类型
        self.centerWord = None  # 中心词汇,例如人物表，中心词汇就是人名，如“李渊”

        self.getAbsolutePosition()  # 获取表格单元的绝对位置
        self.initialNormal()  # 判断表格是否正常
        self.initialTableItemsType()  # 初始化表格单元的类型

    def getAbsolutePosition(self):
        """
        获得表格中每个项目所在的绝对位置，其中行绝对位置为self.absoluteRow,列绝对位置为self.absoluteCol
        :return:
        """
        positionList = []
        for i in range(len(self.Table)):
            colIndex = 0
            before = 0  # 记录从这一行开始，到现在，之前有几个元素进入队列
            for j in range(len(self.Table[i])):
                col = self.Table[i][j]
                colStart = 0
                for position in positionList:
                    colStart += position[1]
                col.absoluteCol = colStart + j - before
                col.absoluteRow = i
                if col.rowspan > 1 or col.colspan > 1:
                    positionList.append([col.rowspan, col.colspan])
                    before += 1
                colIndex += 1

            for x in reversed(range(len(positionList))):
                if positionList[x][0] > 1:
                    positionList[x][0] -= 1
                else:
                    positionList.pop(x)

    def initialCorrect(self) -> bool:
        """
        判断表格是否正确，正确表格的行与列单位数都非常规整
        :return:
        """
        colLenList = []
        for rows in self.Table:
            colLen = 0
            for col in rows:
                colLen += col.colspan
            colLenList.append(colLen)
        self.__isCorrect = (len(set(colLenList)) == 1)
        return self.__isCorrect

    def initialNormal(self):
        """
        判断是否是一个正常的表格，正常表格必须行列数都大于2
        :return:
        """
        if self.rowNumber >= 2 and self.colNumber >= 2:
            self.__isNormal = True
        else:
            self.__isNormal = False
        return self.__isNormal

    def isCorrect(self):
        """
        判断当前表格是否正确，即行列单元数相同
        :return:
        """
        return self.__isCorrect

    def isNormal(self):
        """
        判断当前表格是否正常，即行列数均大于等于2
        :return:
        """
        return self.__isNormal

    def flip(self):
        """
        翻转表格方向,并返回一个新的矩阵
        :return:
        """
        newTable = Table(rowNumber=self.colNumber, colNumber=self.rowNumber, name=self.name)
        for i in range(self.rowNumber):
            for j in range(self.colNumber):
                newTable.Table[j][i] = deepcopy(self.Table[i][j])
        if self.unfoldDirection == "ROW":
            newTable.unfoldDirection = "COL"
        if self.unfoldDirection == "COL":
            newTable.unfoldDirection = "ROW"

        newTable.prefix = self.prefix  # 表格的前驱
        newTable.propertyList = self.propertyList  # 属性单元列表
        newTable.propertyNameList = self.propertyNameList  # 属性名列表
        newTable.propertyLineNum = self.propertyLineNum  # 属性行数
        newTable.tableType = self.tableType  # 表格类型
        newTable.centerWord = self.centerWord  # 中心词汇,例如人物表，中心词汇就是人名，如“李渊”
        newTable.initialNormal()  # 判断表格是否正常
        newTable.initialCorrect()
        return newTable

    def changeToStr(self):
        """
        将表格内的数据形式全部转化为字符串
        :return:
        """
        for i in range(self.rowNumber):
            for j in range(self.colNumber):
                self.Table[i][j].content = str(self.Table[i][j].content)
        return self

    def getLengthCharacter(self):
        """
        计算矩阵的几何特征，返回行方差均值和列方差均值，方差越小，则按照该方式展开的可能性越大
        :return: 方差均值和列方差均值
        """
        data = np.zeros((self.rowNumber, self.colNumber), dtype=int)
        for i in range(self.rowNumber):
            for j in range(self.colNumber):
                data[i, j] = len(str(self.Table[i][j].content))

        rowVarianceMean = np.mean(np.std(data, axis=0))  # 行方差均值
        colVarianceMean = np.mean(np.std(data, axis=1))  # 列方差均值
        sumNumber = rowVarianceMean + colVarianceMean
        if sumNumber == 0:
            return rowVarianceMean, colVarianceMean
        return rowVarianceMean / sumNumber, colVarianceMean / sumNumber

    def getTypeCharacter(self):
        """
        计算矩阵的类型特征，返回行方差均值和列方差均值，方差越小，则按照该方式展开的可能性越大
        :return: 方差均值和列方差均值
        """
        _typeTree = TypeTree()
        return _typeTree.getTypeCharacter(self)

    def getRowAt(self, row: int):
        """
        获取表格第row行的数据列表,如果获取不到则抛出异常
        :param row: 行数，从0开头
        :return: 第row行对应的数据列表
        """
        if self.__isNormal and self.__isCorrect:
            if 0 <= row < self.rowNumber:
                return self.Table[row]
            else:
                raise Exception(f"row={row},此时超出表格索引范围")
        else:
            raise Exception(f"当前表格不正常，无法获取第{row}行的数据列表")

    def getColAt(self, col: int):
        """
        获取表格第col列的数据列表,如果获取不到则抛出异常
        :param col: 列数，从0开头
        :return: 第col列对应的数据列表
        """
        if self.__isNormal and self.__isCorrect:
            if 0 <= col < self.colNumber:
                res = []
                for row in range(self.rowNumber):
                    res.append(self.Table[row][col])
                return res
            else:
                raise Exception(f"col={col},此时超出表格索引范围")
        else:
            raise Exception(f"当前表格不正常，无法获取第{col}列的数据列表")

    def extendTable(self):
        """
        将当前表格扩展为规范表格
        :return: 扩展后的表格
        """
        # 行扩展
        for rows in self.Table:
            before = 0
            for item in rows:
                if item.rowspan > 1:
                    rowspan = item.rowspan
                    item.rowspan = 1
                    for row in range(item.absoluteRow + 1, item.absoluteRow + rowspan):
                        newItem = deepcopy(item)
                        newItem.rowLoc = row
                        newItem.absoluteRow = row
                        self.Table[row].insert(before, newItem)
                    # before += 1
                before += 1
        # 列扩展
        for rows in self.Table:
            for item in rows:
                if item.colspan > 1:
                    colspan = item.colspan
                    item.colspan = 1
                    for col in range(item.absoluteCol + 1, item.absoluteCol + colspan):
                        newItem2 = deepcopy(item)
                        newItem2.colLoc = col
                        newItem2.absoluteCol = col
                        self.Table[item.absoluteRow].insert(item.absoluteCol, newItem2)
        self.initialNormal()
        self.initialCorrect()
        return self

    def getUnfoldDirection(self) -> str:
        """
        返回表格的展开方向,只能判断为横向展开或者纵向展开
        :return: "ROW"表示横向展开，"COL"表示纵向展开
        """
        if self.unfoldDirection:
            return self.unfoldDirection
        rowRes = [item.tagName == 'th' for item in self.getRowAt(0)]
        if rowRes[0] and len(set(rowRes)) == 1:
            self.unfoldDirection = "ROW"
            return self.unfoldDirection
        colRes = [item.tagName == 'th' for item in self.getColAt(0)]
        if colRes[0] and len(set(colRes)) == 1:
            self.unfoldDirection = "COL"
            return self.unfoldDirection

        rowVarianceMean, colVarianceMean = self.getLengthCharacter()
        rowTypeCharacter, colTypeCharacter = self.getTypeCharacter()
        W1 = 0.5
        W2 = 0.5
        Row = W1 * rowVarianceMean + W2 * rowTypeCharacter
        Col = W1 * colVarianceMean + W2 * colTypeCharacter
        if Row < Col:
            direction = "ROW"
        else:
            direction = "COL"
        self.unfoldDirection = direction
        return self.unfoldDirection

    def __tagDiscriminatePropertyLineNum(self, direction: str):
        res = 0
        if direction == "ROW":
            for i in range(self.rowNumber):
                for j in range(self.colNumber):
                    item = self.Table[i][j]
                    if item.tagName != "th":
                        return res
                res += 1
            return res
        elif direction == "COL":
            for j in range(self.colNumber):
                for i in range(self.rowNumber):
                    item = self.Table[i][j]
                    if item.tagName != "th":
                        return res
                res += 1
            return res
        else:
            raise Exception(f"不存在这种表格展开方向<{direction}>")

    def __typeDiscriminatePropertyLineNum(self, direction: str) -> int:
        """
        根据类型判断属性行列数
        :param direction: 展开方向，目前有"ROW"，即行展开，和"COL"，即列展开
        :return: 属性行列数 n，若无法判别，则返回 0
        """
        characterTypeSet = {"字符类型", "中文", "英文", "大写", "小写", "大小写混合"}
        res = 0
        if direction == "ROW":
            for i in range(self.rowNumber):
                for j in range(self.colNumber):
                    item = self.Table[i][j]
                    if item.type_ not in characterTypeSet:
                        return res
                res += 1
            if res == self.rowNumber:  # 如果遍历了所有行
                res = 0
        elif direction == "COL":
            for i in range(self.colNumber):
                for j in range(self.rowNumber):
                    item = self.Table[j][i]
                    if item.type_ not in characterTypeSet:
                        return res
                res += 1
            if res == self.colNumber:  # 如果遍历了所有列
                res = 0
        else:
            raise Exception(f"不存在这种表格展开方向<{direction}>")
        return res

    def __treeDepthDiscriminatePropertyLineNum(self, direction: str):
        """
        根据数深度判别属性行列数。
        该方法仅在根据类型判别行列数(typeDiscriminatePropertyLineNum)失效后才使用。
        :return:
        """
        res = 1
        if direction == "ROW":
            for i in range(self.rowNumber):
                for j in range(self.colNumber):
                    item = self.Table[i][j]
        elif direction == "COL":
            pass
        else:
            pass
        return res

    def discriminatePropertyLineNum(self, direction: str):
        if self.propertyLineNum:
            return self.propertyLineNum
        res = self.__tagDiscriminatePropertyLineNum(direction)
        if res == 0 or res > 2:
            res = self.__typeDiscriminatePropertyLineNum(direction)
            if res == 0:
                res = self.__treeDepthDiscriminatePropertyLineNum(direction)
        self.propertyLineNum = res
        return self.propertyLineNum

    def initialTableItemsType(self):
        """
        初始化表格每一个单元的类型，如“你好”就是中文，“123”就是数字>1，“hello”就是英文
        :return:
        """
        for row in self.Table:
            for item in row:
                item.getTableItemType()

    def initialTableItemWordType(self):
        """
        获得单词类型，例如"水果"就是名词，“跑步”就是动词，如果是句子就会划分为多个词
        :return:
        """
        for row in self.Table:
            for item in row:
                item.getTableItemWordType()

    def writeTable2Doc(self, filepath: str):
        """
        将表格写入到指定路径的doc文档中
        :param table: 导入的表格
        :param filepath: 指定的文件路径
        :return:
        """
        if os.path.exists(filepath):
            doc = Document(filepath)
        else:
            doc = Document()
        # 设置字体样式
        doc.styles['Normal'].font.name = u'宋体'
        doc.styles['Normal'].element.rPr.rFonts.set(qn('w:eastAsia'), u'宋体')
        # ------添加文档标题-------
        paragraph = doc.add_paragraph()
        run = paragraph.add_run(self.name)
        font = run.font
        # 设置字体大小
        font.size = Pt(12)
        # 设置水平居中
        paragraph_format = paragraph.paragraph_format
        paragraph_format.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        addTable = doc.add_table(rows=self.rowNumber, cols=self.colNumber, style="Table Grid")
        addTable.cell(0, 0).paragraphs[0].paragraph_format.alignment = WD_TABLE_ALIGNMENT.CENTER  # 第一行表格水平居中
        self.getAbsolutePosition()
        for rowData in self.Table:
            for item in rowData:
                addTable.cell(item.absoluteRow, item.absoluteCol).merge(
                    addTable.cell(item.absoluteRow + item.rowspan - 1,
                                  item.absoluteCol + item.colspan - 1))
                addTable.cell(item.absoluteRow, item.absoluteCol).text = item.content
        doc.save(filepath)

    @except_output()
    def __table2DictList(self, filtration=False, deletePersonName=False) -> list:
        """
        表格转化为字典列表,默认为横向展开
        :return:
        """
        if self.__isNormal and self.__isCorrect:
            if deletePersonName:
                personNameIndex = self.__getPersonNameIndex()
                self.deleteOneCol(personNameIndex)
            jsonStrList = []
            direction = self.getUnfoldDirection()
            lineNum = self.discriminatePropertyLineNum(direction)
            if lineNum >= 1:
                heads = [str(head.content) for head in self.getRowAt(lineNum - 1)]
                for i in range(lineNum, self.rowNumber):
                    jsonStr = {}
                    for j in range(self.colNumber):
                        item = self.Table[i][j]
                        if filtration:
                            if str(item.content).isspace() or len(str(item.content)) == 0:
                                continue
                        jsonStr[heads[j]] = str(item.content)
                    jsonStrList.append(jsonStr)
                return jsonStrList
        else:
            raise Exception("该表格不规范，无法写成json串")

    def table2Json(self) -> str:
        """
        表格转化为json串
        :return:
        """
        return json.dumps(self.__table2DictList(), ensure_ascii=False)

    def getTableType(self):
        """
        识别表格类型
        :return:
        """
        if self.tableType:
            return self.tableType
        else:
            if self.__isPersonInfoTable():
                self.tableType = "个人信息表"
            elif self.__isPropertyRelationShipTable():
                self.tableType = "属性关系表"
            elif self.__isTitleRelationShipTable():
                self.tableType = "标题关系表"
            elif self.__isEntityRelationshipTable():
                self.tableType = "实体关系表"
            else:
                self.tableType = "其他"
        return self.tableType

    def __isPersonInfoTable(self):
        """
        识别当前表格是否为人物信息表格
        :return:
        """
        if self.getUnfoldDirection() == "ROW":
            if self.rowNumber != 2:
                return False
        elif self.getUnfoldDirection() == "COL":
            if self.colNumber != 2:
                return False
        firstProperty = {"中文名", "本名"}
        if str(self.Table[0][0].content) in firstProperty:  # 判断第一行第一列的属性名
            self.fusionJsonWord(f"{gol.get_value('personTablePath')}\\personInfo.json")  # 融合属性
            return True

        Threshold = 0.5
        personTablePath = gol.get_value('personTablePath')
        filename = "personInfo.json"
        personProperty = FileIO.readJson(os.path.join(personTablePath, filename))
        personPropertySet = set(personProperty)
        tablePropertySet = set(self.getPropertyList(isPropertyName=True))
        if len(tablePropertySet) == 0:
            proportion = 0
        else:
            proportion = len(tablePropertySet.intersection(personPropertySet)) / len(tablePropertySet)
        if proportion >= Threshold:
            return True
        else:
            return False

    def __isPropertyRelationShipTable(self) -> bool:
        """
        判断是否为属性关系表
        :return:
        """
        personTablePath = gol.get_value('personTablePath')
        propertyRelationShipList = FileIO.readJson(f"{personTablePath}\\propertyRelationship.json")
        propertyRelationshipSet = set(propertyRelationShipList)
        propertyList = self.getPropertyList(isPropertyName=True)
        for propertyName in propertyList:
            if propertyName in propertyRelationshipSet:
                return True
        return False

    def __isTitleRelationShipTable(self) -> bool:
        """
        判断是否为标题关系表
        :return:
        """
        if self.name:
            personTablePath = gol.get_value('personTablePath')
            relationshipList = FileIO.readJson(f"{personTablePath}\\captionRelationship.json")
            for relationship in relationshipList:
                if relationship in self.name:
                    return True
            reg = re.compile(r".*[\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]+.*")
            if re.match(reg, str(self.name)):
                return True
        return False

    def __isEntityRelationshipTable(self) -> bool:
        """
        判断是否为实体关系表
        :return:
        """
        personTablePath = gol.get_value('personTablePath')
        relationshipList = FileIO.readJson(f"{personTablePath}\\personName.json")
        propertyList = self.getPropertyList(isPropertyName=True)
        for propertyName in propertyList:
            for relationship in relationshipList:
                if relationship in propertyName:
                    return True
        return False

    def deleteOneRow(self, index: int):
        """
        删除指定行
        :param index:要删除的索引号，例如Index=0代表第1行
        :return:
        """
        if self.__isCorrect and self.__isNormal:
            if index < 0 or index >= self.rowNumber:
                raise Exception(f"要删除的行<{index}>超出行数范围<0,{self.rowNumber - 1}>")
            del self.Table[index]
            self.rowNumber -= 1
            self.getAbsolutePosition()
            self.initialPropertyList()
        else:
            raise Exception("当前表格未规整，无法删除行")

    def deleteOneCol(self, index: int):
        """
        删除指定列
        :param index: 要删除的索引号，例如Index=0代表第1列
        :return:
        """
        if self.__isCorrect and self.__isNormal:
            if index < 0 or index >= self.colNumber:
                raise Exception(f"要删除的列<{index}>超出列数范围<0,{self.colNumber - 1}>")
            for i in range(self.rowNumber):
                del self.Table[i][index]
            self.getAbsolutePosition()
            self.colNumber -= 1
            self.initialPropertyList()
        else:
            raise Exception("当前表格未规整，无法删除列")

    def __getPropertyRelationshipList(self):
        """
        判断当前表是否为属性关系表
        :return:
        """
        personTablePath = gol.get_value('personTablePath')
        propertyRelationshipList = FileIO.readJson(f"{personTablePath}\\propertyRelationship.json")
        propertyList = self.getPropertyList(isPropertyName=True)
        indexAndNameList = []
        for propertyName in propertyList:
            indexAndNameList.extend(
                [(index, relationshipName) for index, relationshipName
                 in list(enumerate(propertyRelationshipList))
                 if propertyName == relationshipName])
        sortIndexList = sorted(indexAndNameList, key=lambda indexAndNum: indexAndNum[0])  # 根据序号排序
        sortIndexList = [indexAndName[1] for indexAndName in sortIndexList]
        return sortIndexList

    def fusionJsonWord(self, jsonFilePath):
        """
        融合个人属性到“个人信息表”列表之中,使得下一次的判断更加精确
        :return:
        """
        personProperty = FileIO.readJson(jsonFilePath)
        personPropertySet = set(personProperty)
        tablePropertySet = set(self.getPropertyList(isPropertyName=True))
        personPropertySet = personPropertySet.union(tablePropertySet)
        FileIO.write2Json(list(personPropertySet), jsonFilePath)

    def getPropertyList(self, isPropertyName=False):
        """
        获取属性列
        :return:
        """
        if not isPropertyName:
            if self.propertyList:
                return self.propertyList
        else:
            if self.propertyNameList:
                return self.propertyNameList
        self.initialPropertyList()
        if not isPropertyName:
            return self.propertyList
        else:
            self.propertyNameList = [str(item.content) for item in self.propertyList]
            return self.propertyNameList

    def initialPropertyList(self):
        direction = self.getUnfoldDirection()
        propertyLineNum = self.discriminatePropertyLineNum(direction)
        if direction == "ROW":
            self.propertyList = self.getRowAt(propertyLineNum - 1)
        elif direction == "COL":
            self.propertyList = self.getColAt(propertyLineNum - 1)
        else:
            raise Exception(f"不存在该表格展开方向<{direction}>")
        self.propertyNameList = [str(p.content) for p in self.propertyList]

    @staticmethod
    def __notNullAppend(aList: list, a: str, b: str, c: str, isName=False):
        """
        非空添加三元组到列表中，将[a,b,c]添加到列表，如果a，b,c中任意一个为空，则不添加,若长度太长，也不会添加
        :param aList:待添加的列表
        :param a:主体str
        :param b:关系str
        :param c:客体str
        :param isName:第三个属性是否为人名
        :return:
        """
        if len(a) == 0 or a.isspace() or len(b) == 0 or b.isspace() or len(c) == 0 or c.isspace():
            return
        if len(a) > 7 or len(b) > 7:  # 默认人物名，关系名不超过7个字符
            return
        myList = [a, b, c]
        for i in range(len(myList)):
            myList[i] = re.sub(u"\(.?\)|\\（.*?）|\\{.*?}|\\[.*?]|\\【.*?】||\\<.*?\\>", "", myList[i])  # 去除括号
        punctuation = "[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？?、~@#￥%……&*（）]+"
        myList[0] = re.sub(punctuation, "", myList[0])  # 去除人名和关系名中的  符号
        myList[1] = re.sub(punctuation, "", myList[1])
        if isName:
            myList[2] = re.sub(punctuation, "", myList[2])
            if len(myList[2]) > 7:
                return
        aList.append([myList[0], myList[1], myList[2]])

    @except_output("表格专化为三元组时出错")
    def extractEntityRelationship(self):
        """
        抽取实体关系
        :return:
        """
        entity = []
        relationship = []
        typeName = self.getTableType()
        if typeName == "个人信息表" or typeName == "实体关系表":
            entity = self.extractEntity()
        elif typeName == "属性关系表":
            relationship = self.extractPropertyRelationship()
            entity = self.extractEntity()
        elif typeName == "标题关系表":
            relationship = self.extractCaptionRelationship()
            entity = self.extractEntity()
        else:  # 其他表
            self.writeTable2Doc(f"{gol.get_value('tableDocPath')}\\未抽取三元组的表格.docx")
        return [entity, relationship]

    def extractPropertyRelationship(self):
        relationship = []
        if not self.prefix:
            return relationship
        propertyNameList = self.getPropertyList(isPropertyName=True)
        propertyRelationshipList = self.__getPropertyRelationshipList()  # 属性关系列表，例如[关系,辈分]
        if len(propertyRelationshipList) == 0:  # 如果不存在属性关系，则返回空
            return relationship
        if len(propertyRelationshipList) >= 1:  # 如果存在多个属性关系，则删除其余低级的属性关系
            for i in range(1, len(propertyRelationshipList)):
                self.deleteOneCol(propertyNameList.index(propertyRelationshipList[i]))
            propertyNameList = self.getPropertyList(isPropertyName=True)  # 删除属性列之后更新一下属性列表
        personNameIndex = self.__getPersonNameIndex()
        if personNameIndex != -1:
            personNameList = [str(person.content) for person in self.getColAt(personNameIndex)]  # 获得人名列表
            index = propertyNameList.index(propertyRelationshipList[0])
            relationshipList = [str(relationship.content) for relationship in self.getColAt(index)]  # 获得关系名列表
            self.deleteOneCol(index)  # 删除关系名列表
            propertyLineNum = self.discriminatePropertyLineNum(self.getUnfoldDirection())
            for i in range(propertyLineNum, self.rowNumber):
                # 构建三元组
                self.__notNullAppend(relationship, self.prefix, relationshipList[i], personNameList[i],
                                     isName=True)
        return relationship

    def extractCaptionRelationship(self):
        """
        将标题关系转化为三元组
        :return:
        """
        relationship = []
        if self.name and self.prefix:
            personNameIndex = self.__getPersonNameIndex()
            if personNameIndex != -1:
                personNameList = [str(person.content) for person in self.getColAt(personNameIndex)]  # 获得人名列表
                for i in range(len(personNameList)):
                    # 添加三元组
                    self.__notNullAppend(relationship, self.prefix, self.name, personNameList[i], isName=True)
        return relationship

    def extractEntity(self, getEntityTriad=False):
        """
        将实体表转化为与人物有关的实体
        :return:
        """
        entity = []
        personNameIndex = self.__getPersonNameIndex()
        if personNameIndex == -1:
            return entity
        personNameList = [str(person.content) for person in self.getColAt(personNameIndex)]  # 获得人名列表
        if getEntityTriad:
            self.deleteOneCol(personNameIndex)  # 删除人名列表
            if self.colNumber >= 1:
                propertyIndex = self.discriminatePropertyLineNum(self.getUnfoldDirection()) - 1
                propertyNameList = self.getPropertyList(isPropertyName=True)  # 获取除了人名之外的，其余的属性列表
                propertyLine = self.discriminatePropertyLineNum(self.getUnfoldDirection())  # 属性行所占的行数
                for i in range(propertyLine, self.rowNumber):
                    for j in range(propertyIndex, self.colNumber):
                        content = str(self.Table[i][j].content)
                        # 添加三元组
                        self.__notNullAppend(entity, personNameList[i], propertyNameList[j], content)
        else:
            dictList = self.__table2DictList(filtration=True,deletePersonName=True)
            personNameList = self.__clearPersonNameList(personNameList)
            if len(personNameList) == len(dictList):
                for i in range(len(personNameList)):
                    if len(personNameList[i]) == 0 or str(personNameList[i]).isspace():  # 姓名为空则跳过
                        continue
                    entity.append([personNameList[i], dictList[i]])

        return entity

    def __getPersonNameIndex(self):
        """
        返回人名所在的列的索引
        :return:
        """

        def _getListIndex(name: str, stringList: list):
            __index = 0
            for string in stringList:
                if string in name:
                    return __index
                else:
                    __index += 1
            return -1

        personNameIndex = -1
        personTablePath = gol.get_value('personTablePath')
        relationshipList = FileIO.readJson(f"{personTablePath}\\personName.json")
        propertyList = self.getPropertyList(isPropertyName=True)
        index = 0
        for propertyName in propertyList:
            personNameIndex = _getListIndex(propertyName, relationshipList)
            if personNameIndex != -1:
                personNameIndex = index
                break
            index += 1
        return personNameIndex

    @staticmethod
    def __clearPersonNameList(personNameList: list):
        punctuation = "[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？?、~@#￥%……&*（）]+"
        personNameList.pop(0)  # 删除第一个元素
        for i in range(len(personNameList)):
            personNameList[i] = re.sub(u"\(.?\)|\\（.*?）|\\{.*?}|\\[.*?]|\\【.*?】||\\<.*?\\>", "",
                                       personNameList[i])  # 去除括号
            personNameList[i] = str(personNameList[i]).split("/")[0]
            personNameList[i] = re.sub(punctuation, "", personNameList[i])
        return personNameList

    def clearTable(self):
        """
        清理表格，去除表格中无意义的序号，去除空行或者空列
        :return:
        """
        propertyList = self.getPropertyList(isPropertyName=True)
        # 清除带有“序”的属性行
        clearSet = ["序号", "序"]
        indexes = [index for index, propertyName in enumerate(propertyList) if propertyName in clearSet]
        if indexes:
            if self.getUnfoldDirection() == "ROW":
                self.deleteOneCol(indexes[0])
            else:
                self.deleteOneRow(indexes[0])
            self.getAbsolutePosition()
        # 如果第一行内容为空，则删除第一行
        canContinue = True
        for item in self.getRowAt(self.rowNumber - 1):
            if not (len(str(item.content)) == 1 or str(item.content).isspace()):
                canContinue = False
            if not canContinue:
                break
        if canContinue:
            self.deleteOneRow(self.rowNumber - 1)
        # 如果最后一行是参考资料，删除这一行
        canContinue = True
        for item in self.getRowAt(self.rowNumber - 1):
            if "参考资料" not in str(item.content):
                canContinue = False
            if not canContinue:
                break
        if canContinue:
            self.deleteOneRow(self.rowNumber - 1)

        # 将表格中的单纯的符号单元转化为空格
        for i in range(self.rowNumber):
            for j in range(self.colNumber):
                if self.Table[i][j].getTableItemType() == "标点类型":
                    self.Table[i][j].content = ""


class TypeTree:
    """
    类型树类
    """

    def __init__(self):
        tree = Tree()
        tree.create_node(tag="类型", identifier="类型")
        tree.create_node(tag="超链接", identifier="超链接", parent="类型")
        tree.create_node(tag="图片", identifier="图片", parent="类型")
        tree.create_node(tag="字符和数字", identifier="字符和数字", parent="类型")
        tree.create_node(tag="其他类型", identifier="其他类型", parent="类型")
        tree.create_node(tag="标点类型", identifier="标点类型", parent="类型")
        tree.create_node(tag="字符类型", identifier="字符类型", parent="字符和数字")
        tree.create_node(tag="数字类型", identifier="数字类型", parent="字符和数字")
        tree.create_node(tag="中文", identifier="中文", parent="字符类型")
        tree.create_node(tag="英文", identifier="英文", parent="字符类型")
        tree.create_node(tag="<=0", identifier="<=0", parent="数字类型")
        tree.create_node(tag="0-1", identifier="0-1", parent="数字类型")
        tree.create_node(tag=">=1", identifier=">=1", parent="数字类型")
        tree.create_node(tag="大写", identifier="大写", parent="英文")
        tree.create_node(tag="小写", identifier="小写", parent="英文")
        tree.create_node(tag="大小写混合", identifier="大小写混合", parent="英文")
        # tree.show()
        self.tree = tree

    def getTypeCharacter(self, table: Table) -> Tuple[float, float]:
        """
        计算表格的行类型特征和列类型特征
        :param table:传入的表格
        :return:行类型特征rowTypeCharacter和列类型特征colTypeCharacter
        """
        rowTypeCharacter = 0
        colTypeCharacter = 0
        n = min(table.rowNumber, table.colNumber)
        # for i in range(n - 1):
        #     rowTypeCharacter += self.VType(table.getRowAt(i), table.getRowAt(n - 1))
        #     colTypeCharacter += self.VType(table.getColAt(i), table.getColAt(n - 1))
        for i in range(table.rowNumber - 1):
            rowTypeCharacter += self.VType(table.getRowAt(i), table.getRowAt(table.rowNumber - 1))
        rowTypeCharacter = rowTypeCharacter / (table.rowNumber - 1)

        for j in range(table.colNumber - 1):
            colTypeCharacter += self.VType(table.getColAt(j), table.getColAt(table.colNumber - 1))
        colTypeCharacter = colTypeCharacter / (table.colNumber - 1)
        sumNumber = rowTypeCharacter + colTypeCharacter
        if sumNumber == 0:
            return rowTypeCharacter, colTypeCharacter
        return rowTypeCharacter / sumNumber, colTypeCharacter / sumNumber

    def _VType(self, item1: TableItem, item2: TableItem) -> int:
        """
        计算两个表格单元之间的类型差异距离
        :param item1: 表格单元1
        :param item2: 表格单元2
        :return: 类型差异距离
        """
        distance = 0
        typeNode1 = item1.type_
        typeNode2 = item2.type_
        if typeNode1 is None or typeNode2 is None:
            raise Exception("当前类型为None，无法计算出类型之间的距离")
        level1 = self.tree.depth(typeNode1)
        level2 = self.tree.depth(typeNode2)
        if typeNode1 == typeNode2:
            return distance
        if level1 > level2:
            while level1 != level2:
                typeNode1 = self.tree.parent(typeNode1).identifier
                distance += 1
                level1 -= 1
        elif level2 > level1:
            while level1 != level2:
                typeNode2 = self.tree.parent(typeNode2).identifier
                distance += 1
                level2 -= 1
        if level1 == level2:
            while typeNode1 != typeNode2:
                typeNode1 = self.tree.parent(typeNode1).identifier
                typeNode2 = self.tree.parent(typeNode2).identifier
                distance += 2
        return distance

    def VType(self, v1: list, v2: list) -> float:
        res = 0
        len1 = len(v1)
        len2 = len(v2)
        if len1 == 0 or len2 == 0:
            return res
        m = min(len1, len2)
        for i in range(m):
            res += self._VType(v1[i], v2[i])
        return res / m


class TableFeatures:
    """表格特征"""

    def __init__(self):
        self.averageColNum = 0  # 平均列数
        self.averageRowNum = 0  # 平均行数
        self.colNumStandardDeviation = 0  # 列数标准偏差
        self.rowNumStandardDeviation = 0  # 行数标准偏差
        self.averageItemLength = 0  # 平均单元格长度
        self.itemLengthStandardDeviation = 0  # 单元格长度标准偏差


def changeTig2Table(tag: Tag, caption=None, prefix=None) -> Table:
    """
    将tag标签转化为table数据结构
    :param prefix: 前缀
    :param caption:标题
    :param tag: 输入的标签
    :return: 返回表格
    """

    table = Table()
    table.Table = []
    rowMaxSize = 0
    colLenList = []
    colIndex = rowIndex = 0
    table.name = caption  # 命名
    table.prefix = prefix
    for rowData in tag.children:
        innerList = []
        colSize = 0
        for colData in rowData.children:
            if isinstance(colData, NavigableString):
                continue
            rowspan = colspan = 1
            # 获取表格单元中的超链接
            href = {}
            aList = colData.find_all("a")
            for a in aList:
                if a.has_attr("href"):
                    href[a.text] = r"https://baike.baidu.com" + a["href"]
            # 获取表格单元中的图片
            imgSrc = []
            imgList = colData.find_all("img")
            for img in imgList:
                if img.has_attr("src"):
                    imgSrc.append(img["src"])
            # 获取表格的占据行列数
            if colData.has_attr("rowspan"):
                rowspan = int(colData['rowspan'])
            if colData.has_attr("colspan"):
                colspan = int(colData['colspan'])

            colSize += colspan
            text = re.sub('(\[)\d+(\])', '', colData.text)  # 去除索引注释，例如 [12]
            content = text.replace("\xa0", "")
            tagName = colData.name
            tableItem = TableItem(content, rowIndex, rowspan, colIndex, colspan, href, imgSrc, tagName=tagName)
            innerList.append(tableItem)
        colLenList.append(colSize)
        table.Table.append(innerList)
        rowMaxSize += 1
    table.colNumber = max(colLenList)
    table.rowNumber = rowMaxSize
    table.getAbsolutePosition()
    table.initialNormal()  # 判断是否正常
    table.initialCorrect()  # 判断是否正确
    table.initialTableItemsType()  # 初始化表格单元的类型
    # table.initialTableItemWordType()  # 初始化表格单词类型
    return table


if __name__ == "__main__":
    typeTree = TypeTree()