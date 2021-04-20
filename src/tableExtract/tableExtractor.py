import json
import re
import time
from queue import Queue
from urllib.parse import unquote

from bs4 import BeautifulSoup, Comment
from bs4.element import Tag, NavigableString
from docx import Document
from pyhanlp import HanLP  # 使用前导入 HanLP工具

import src.app.gol as gol
from src.IO.databaseInteraction.MSSQL import SqlServerProcessor
from src.IO.fileInteraction.FileIO import FileIO
from src.spider.WebSpider import WebSpider
from src.tableExtract.table import changeTig2Table, Table, TableItem, changeWordTable2Table
from src.tools.algorithm.exceptionCatch import except_output


class TableExtract:
    def __init__(self):
        self.segment = HanLP.newSegment()
        # 名词属性列表
        self.attrTypeSet = frozenset(
            ["n", "nb", "nba", "nbc", "nbp", "nf", "ng", "nh", "nhd", "nhm", "ni", "nic", "nis", "nit", "nl", "nm",
             "nmc", "nn", "nnd", "nnt", "nr", "nr1", "nr2", "nrf", "nrj", "ns", "nsf", "nt", "ntc", "ntcb", "ntcf",
             "ntch", "nth", "nto", "nts", "ntu", "nx", "nz"])
        self.nowName = ''
        self.nowUrl = ''

    def test(self):
        _tableDocPath = gol.get_value("tableDocPath")
        _jsonPath = gol.get_value("jsonPath")
        entityAndRelationshipPath = gol.get_value("entityAndRelationshipPath")
        # 这是一个测试
        tempUrl = [
            r"https://baike.baidu.com/item/%E8%B5%B5%E5%8C%A1%E8%83%A4/61716",  # 赵匡胤
            r"https://baike.baidu.com/item/%E5%AD%94%E5%AD%90/1584"
        ]
        sql = SqlServerProcessor()
        sql.clearAllTable()
        for url in tempUrl:
            html = WebSpider.getHtml(url)
            last = unquote(url.split('/')[-1])
            self.nowName = unquote(url.split('/')[-2]) if last.isdigit() else last
            self.nowUrl = url
            _tableList = self.getTable(html)
            for table in _tableList:
                table.hrefMap[self.nowName] = self.nowUrl
                table = table.extendTable()
                table.prefix = self.nowName
                if table.getUnfoldDirection() == "COL":  # 把表格全部变成横向展开的
                    table = table.flip()
                table.clearTable()
                entity, relationship = table.extractEntityRelationship()
                if len(entity) != 0 or len(relationship) != 0:
                    entityJson = json.dumps(entity, ensure_ascii=False)
                    relationshipJson = json.dumps(relationship, ensure_ascii=False)
                    sql.writeER2DB('entityAndRelationship', entityJson, relationshipJson)
            pass

    @except_output()
    def start(self, maxWaitTimes=10000):
        _tableDocPath = gol.get_value("tableDocPath")
        _jsonPath = gol.get_value("jsonPath")
        entityAndRelationshipPath = gol.get_value("entityAndRelationshipPath")
        maxSize = 200
        pendingQueue = Queue(maxSize)
        sql = SqlServerProcessor()
        waitTimes = maxWaitTimes  # 100s时间的等待,如果100s结束了，数据库中都没有数据，那么终止该程序
        while waitTimes:
            for data in sql.getUrlAndHtmlFromDB("personUrlAndHtml", int(maxSize / 2)):
                pendingQueue.put(data)
            sql.deleteFromDBWithIdNum("personUrlAndHtml", int(maxSize / 2))

            while not pendingQueue.empty():
                waitTimes = maxWaitTimes
                dataTuple = pendingQueue.get_nowait()
                url = dataTuple[0]
                html = dataTuple[1]
                last = unquote(url.split('/')[-1])
                self.nowName = unquote(url.split('/')[-2]) if last.isdigit() else last
                self.nowUrl = url
                _tableList = self.getTable(html)
                for table in _tableList:
                    table.hrefMap[self.nowName] = self.nowUrl
                    table = table.extendTable()
                    if table.isNormal() and table.isCorrect():
                        if table.getUnfoldDirection() == "COL":  # 把表格全部变成横向展开的
                            table = table.flip()
                        # table.writeTable2Doc(f"{gol.get_value('tableDocPath')}\\{name}__规整后.docx")
                        table.clearTable()
                        entity, relationship = table.extractEntityRelationship()
                        if len(entity):
                            print(entity)
                        if len(relationship):
                            print(relationship)
                        if len(entity) != 0 or len(relationship) != 0:
                            entityJson = json.dumps(entity, ensure_ascii=False)
                            relationshipJson = json.dumps(relationship, ensure_ascii=False)
                            sql.writeER2DB('entityAndRelationship', entityJson, relationshipJson)

                if pendingQueue.qsize() < int(pendingQueue.maxsize / 2):
                    for data in sql.getUrlAndHtmlFromDB("personUrlAndHtml", int(pendingQueue.maxsize / 2)):
                        pendingQueue.put(data)
                    sql.deleteFromDBWithIdNum("personUrlAndHtml", int(pendingQueue.maxsize / 2))

            time.sleep(1.0)
            waitTimes -= 1

    def getTable(self, _html: str) -> list:
        """
        表格定位
        :param _html:
        :return:
        """
        # 预处理
        _html, _soup = htmlPreTreat(_html)
        _tableList = self.extractNonstandardTable(_soup)
        tagTable = _soup.find_all(name="table")
        for table in tagTable:
            caption, prefix = getCaption(table)
            if self.throughHeuristicRule(table):
                aTable = changeTig2Table(table, caption, prefix)
                _tableList.append(aTable)
        return _tableList

    def throughHeuristicRule(self, table: Tag):
        """
        判断是否通过启发式规则
        :return:
        """

        def _Rule1(tag) -> bool:
            """
            1如果实体信息存在于有<TABLE>标签的表格之中，即<table></table>标签之中，
            那么通常此表格表现为多行多列(大于两行两列）的结构
            :param tag:标签
            :return:，满足该规则记为True，不满足该规则记为False;
            """
            if len(tag.contents) >= 2:
                now = tag.next_element
                if now.name == "caption":
                    now = now.nextSibling
                if len(now.contents) >= 2:
                    return True
            else:
                return False
            return False

        def _Rule2(tag) -> bool:
            """
            2无论对于有<TABLE>标签的表格还是无<TABLE>标签的表格来说，
            其实体信息所在区域中不会包含大量的超链接、表单、图片、脚本或者嵌套的表格，记为b，不满足该规则记为b_;
            :return:
            """
            # 获取超链接
            hrefs = [a['href'] for a in tag.find_all('a', href=True)]
            # 获取表单
            tables = tag.find_all('table')
            sub_table = []
            for _table in tables:
                if type(_table) == Tag:
                    sub_table.append(_table.descendants.find_all('table'))
            scripts = tag.find_all('script')
            img = tag.find_all('img')
            rows = len(tag.contents)
            now = tag.contents[0]
            if now.name == "caption":
                now = now.nextSibling
            cols = len(now.contents)
            if len(hrefs) > rows * cols / 2 or len(sub_table) > 3 or len(scripts) > 1 or len(img) > rows * cols / 2:
                return False
            else:
                return True

        def _Rule3(tableExtractor, tag) -> bool:
            """
            3属性名一般出现在前几行或前几列，记为c，不满足该规则记为c_;
            :param tag:
            :return:
            """
            tagContents = tag.contents
            if len(tagContents) > 2:
                # 判断前2行、前2列是否有属性
                for tagContent in tagContents[0:2]:
                    if tagContent.name == "caption":
                        continue
                    contentList = tagContent.contents
                    for content in contentList:
                        results = list(tableExtractor.segment.seg(content.text))
                        for result in results:
                            if str(result.nature) not in tableExtractor.attrTypeSet:
                                return False
                            return True
                return False
            return False

        return _Rule1(table) and _Rule2(table) and _Rule3(self, table)

    def extractNonstandardTable(self, tag: Tag):
        """
        抽取非规范表格
        :return:
        """
        extractUrl = r"baike.baidu.com"
        urlTableMapPath = gol.get_value("urlTableMapPath")
        ruleDict = FileIO.readJson(f"{urlTableMapPath}\\{extractUrl}.json")
        return self.extractListTable(tag, ruleDict)

    def extractListTable(self, tag: Tag, ruleDict: dict):
        _tableList = []
        if "class" in ruleDict.keys():
            class_name = ruleDict["class"]
            tagsList = tag.find_all(attrs={"class": re.compile(f"{class_name}")})
            for tags in tagsList:
                dlList = tags.find_all("dl")
                dt = []
                dd = []
                for dl in dlList:
                    dt.extend(dl.find_all("dt"))
                    dd.extend(dl.find_all("dd"))

                if len(dt) == len(dd):
                    newTagList = []
                    for i in range(len(dt)):
                        temp = [dt[i], dd[i]]
                        newTagList.append(temp)

                    newTable = []
                    for i in range(len(newTagList)):
                        temp = []
                        for j in range(len(newTagList[0])):
                            _item = newTagList[i][j]
                            href = None
                            img = None
                            if _item.has_attr("href"):
                                href = _item["href"]
                            if _item.has_attr("img"):
                                img = _item["img"]
                            if _item.find("br"):
                                for br in _item.find_all("br"):
                                    br.string = "/"
                            text = str(_item.text).replace("\xa0", "")
                            text = re.sub('(\[)\d+(\])', '', text)
                            tableItem = TableItem(text, i, 1, j, 1, href, img)
                            temp.append(tableItem)
                        newTable.append(temp)
                    if newTable:
                        ATable = Table(len(newTable), 2, table=newTable)
                        ATable.unfoldDirection = "COL"
                        if str(ATable.cell[0][0].content) in ["本名", "中文名"]:
                            ATable.hrefMap[str(ATable.cell[0][1].content)] = self.nowUrl
                            ATable.tableType = "个人信息表"
                        _tableList.append(ATable)
        return _tableList


def getCaption(_tag: Tag):
    """
    提取标签中的表格标题 和 标题前缀
    :param _tag:输入的标签
    :return:表格标题 和 标题前缀
    """
    _prefix = None
    _caption = None
    temp = _tag.find(name="caption")  # 去除标题，标题不计入行列之中
    if temp:
        _caption = temp.text
        [caption.extract() for caption in _tag.find_all(name="caption")]
    else:
        _previous = _tag.previous_sibling
        if _previous:
            title = _previous.find(attrs={"class": re.compile(r"^.*title.*$")})
            if title:
                if len(title.contents) == 2:
                    _prefix = title.find(attrs={"class": re.compile(r"^.*prefix.*$")})
                    if _prefix:
                        _prefix = _prefix.text
                    _caption = title.contents[1]
                elif len(title.contents) == 1:
                    if isinstance(title.contents[0], NavigableString):
                        _caption = title.contents[0]
                    else:
                        _caption = title.contents[0].text
            else:
                if len(_previous.contents) == 1:
                    if isinstance(_previous.contents[0], NavigableString):
                        temp = _previous.contents[0]
                        if 0 < len(temp) < 20:
                            _caption = temp
                    else:
                        temp = _previous.contents[0]
                        if 0 < len(temp) < 20:
                            _caption = temp.text
    return _caption, _prefix


def extractWordTable(filename: str) -> list:
    """
    将word文档中的表格转化为Table类
    :param filename: 文件名，人名
    :return:
    """
    tableDocPath = gol.get_value("tableDocPath")
    docPath = f"{tableDocPath}\\{filename}.docx"
    try:
        doc = Document(docPath)
        tableList = []
        for table in doc.tables:
            tableList.append(changeWordTable2Table(table))
        return tableList
    except Exception as e:
        print("读取 Word 文件失败", e)


def htmlPreTreat(_html: str):
    """
    html预处理，包括去除注释、脚本、文章、代码，返回标准化的html和soup
    :param _html: 输入的html串
    :return: _html为返回的格式化的字符串，_soup为返回的soup
    """
    _html = _html.replace("\r", "").replace("\t", "").replace("\n", "")
    _soup = BeautifulSoup(_html, 'lxml')
    # 去除注释
    [comment.extract() for comment in _soup.findAll(text=lambda text: isinstance(text, Comment))]
    # 去除脚本
    [script.extract() for script in _soup.findAll('script')]
    [style.extract() for style in _soup.findAll('style')]
    # 去除文章
    [article.extract() for article in _soup.find_all('article')]
    # 去除代码
    [code.extract() for code in _soup.find_all('code')]
    # 格式化
    return _html, _soup
