import copy
import time
from pprint import pprint
from queue import Queue
import re

from bs4 import BeautifulSoup, Comment
from bs4.element import Tag, NavigableString
from pyhanlp import HanLP  # 使用前导入 HanLP工具

import src.app.gol as gol
from src.IO.databaseInteraction.MSSQL import SqlServerForSpider
from src.IO.fileInteraction.FileIO import FileIO
from src.spider.WebSpider import WebSpider
from src.tableExtract.table import changeTig2Table, Table, TableItem
from src.tools.algorithm.exceptionCatch import except_output


class TableExtract:
    def __init__(self):
        self.segment = HanLP.newSegment()
        # 名词属性列表
        self.attrTypeSet = frozenset(
            ["n", "nb", "nba", "nbc", "nbp", "nf", "ng", "nh", "nhd", "nhm", "ni", "nic", "nis", "nit", "nl", "nm",
             "nmc", "nn", "nnd", "nnt", "nr", "nr1", "nr2", "nrf", "nrj", "ns", "nsf", "nt", "ntc", "ntcb", "ntcf",
             "ntch", "nth", "nto", "nts", "ntu", "nx", "nz"])

    def test(self):
        _tableDocPath = gol.get_value("tableDocPath")
        _jsonPath = gol.get_value("jsonPath")
        entityAndRelationshipPath = gol.get_value("entityAndRelationshipPath")
        # 这是一个测试
        tempUrl = [
            r"https://baike.baidu.com/item/%E5%B2%B3%E9%A3%9E/127844",  # 岳飞
        ]
        for url in tempUrl:
            html = WebSpider.getHtml(url)
            _tableList = self.getTable(html)
            for table in _tableList:
                table = table.extendTable()
                table.writeTable2Doc(f"{_tableDocPath}\\岳飞展开.docx")
                if table.getUnfoldDirection() == "COL":  # 把表格全部变成横向展开的
                    table = table.flip()
                table.clearTable()
                entityTriad, relationshipTriad = table.extractEntityRelationship()
                pprint(relationshipTriad)
                pprint(entityTriad)
                FileIO.writeTriad2csv(f"{entityAndRelationshipPath}\\entityTriadTest.csv", entityTriad, mode="a+")
                FileIO.writeTriad2csv(f"{entityAndRelationshipPath}\\relationshipTriadTest.csv", entityTriad, mode="a+")

    @except_output()
    def start(self):
        _tableDocPath = gol.get_value("tableDocPath")
        _jsonPath = gol.get_value("jsonPath")
        entityAndRelationshipPath = gol.get_value("entityAndRelationshipPath")
        # self.test()
        maxSize = 200
        pendingQueue = Queue(maxSize)
        sql = SqlServerForSpider()
        waitTimes = 100  # 100s时间的等待,如果100s结束了，数据库中都没有数据，那么终止该程序
        while waitTimes:
            for data in sql.getUrlAndHtmlFromDB("personUrlAndHtml", int(maxSize / 2)):
                pendingQueue.put(data)
            sql.deleteFromDBWithIdNum("personUrlAndHtml", int(maxSize / 2))

            while not pendingQueue.empty():
                waitTimes = 100
                dataTuple = pendingQueue.get_nowait()
                url = dataTuple[0]
                html = dataTuple[1]
                _tableList = self.getTable(html)
                for table in _tableList:
                    table = table.extendTable()
                    if table.isNormal() and table.isCorrect():
                        if table.getUnfoldDirection() == "COL":  # 把表格全部变成横向展开的
                            table = table.flip()
                        table.clearTable()
                        res = table.extractEntityRelationship()
                        if res:
                            entity = res[0]
                            relationship = res[1]
                            pprint(entity)
                            pprint(relationship)
                            # FileIO.writeTriad2csv(f"{entityAndRelationshipPath}\\entity.csv", entity, mode="a+")
                            if relationship:
                                FileIO.writeTriad2csv(f"{entityAndRelationshipPath}\\relationship.csv", relationship,
                                                      mode="a+")
                            if entity:
                                FileIO.write2Json(entity, f"{entityAndRelationshipPath}\\entity.json", "a+", changeLine=True)

                if pendingQueue.qsize() < int(maxSize / 2):
                    for data in sql.getUrlAndHtmlFromDB("personUrlAndHtml", int(maxSize / 2)):
                        pendingQueue.put(data)
                    sql.deleteFromDBWithIdNum("personUrlAndHtml", int(maxSize / 2))

            time.sleep(1.0)
            waitTimes -= 1

    def getTable(self, _html: str) -> list:
        """
        表格定位
        :param _html:
        :return:
        """
        # 预处理
        _html, _soup = self.htmlPreTreat(_html)
        _tableList = self.extractNonstandardTable(_soup)
        tagTable = _soup.find_all(name="table")
        for table in tagTable:
            caption, prefix = self.getCaption(table)
            if self.throughHeuristicRule(table):
                aTable = changeTig2Table(table, caption, prefix)
                _tableList.append(aTable)
        return _tableList

    @staticmethod
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

    @staticmethod
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
                            temp = _previous
                            if 0 < len(temp) < 8:
                                _caption = temp
                        else:
                            temp = _previous.contents[0]
                            if 0 < len(temp) < 8:
                                _caption = temp.text
        return _caption, _prefix

    def throughHeuristicRule(self, table: Tag):
        """
        启发式规则
        :return:
        """
        return self.__Rule1(table) and self.__Rule2(table) and self.__Rule3(table)

    @staticmethod
    def __Rule1(tag) -> bool:
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

    @staticmethod
    def __Rule2(tag) -> bool:
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

    def __Rule3(self, tag) -> bool:
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
                    results = list(self.segment.seg(content.text))
                    for result in results:
                        if str(result.nature) not in self.attrTypeSet:
                            return False
                        return True
            return False
        return False

    def __Rule4(self, tag):
        """
        4包含实体信息的表格，在有<TABLE>标签的表格的属性名，出现的都是字符串形式的内容，不会出现日期、数字或者其它类型的内容，
        而且字符串的长度一般在2-6之间，即使有长度大于6的属性名存在，这样的长属性名也不会超过两个，记为d，不满足该规则记为d ;
        :param tag:
        :return:
        """
        # TODO:需要先确定表格展开方式才能获得属性名
        pass

    def extractNonstandardTable(self, tag: Tag):
        """
        抽取非规范表格
        :return:
        """
        extractUrl = r"baike.baidu.com"
        urlTableMapPath = gol.get_value("urlTableMapPath")
        ruleDict = FileIO.readJson(f"{urlTableMapPath}\\{extractUrl}.json")
        return self.extractListTable(tag, ruleDict)

    @staticmethod
    def extractListTable(tag: Tag, ruleDict: dict):
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
                        # ATable = ATable.flip()
                        ATable.tableType = "个人信息表"
                        _tableList.append(ATable)
        return _tableList

    def extractTablesWithTopic(self, table: Table, topicName: str):
        """
        根据主题识别表格
        :param table:
        :param topicName:
        :return:
        """
        topicPathDict = {"个人信息": f'{gol.get_value("personTablePath")}\\personInfo.json'}
        if topicName not in topicPathDict.keys():
            raise Exception("不存在该主题的映射")
        propertyList = FileIO.readJson(topicPathDict[topicName])
        tablePropertyList = table.getPropertyList()

        pass


def deep(tag: Tag):
    """
    计算该节点到底部的距离
    :param tag:
    :return:
    """
    if isinstance(tag, NavigableString):
        return -1
    maxDeep = -1
    for child in tag.children:
        maxDeep = max(maxDeep, deep(child))
    maxDeep += 1
    return maxDeep


def rootDeep(tag: Tag):
    """
    计算从根到该节点的距离
    :param tag:
    :return:
    """
    res = 0
    now = copy.copy(tag)
    while now is not None:
        res += 1
        now = now.parent
    return res
