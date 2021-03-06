import json
import os
import random
import re
import time
from copy import deepcopy
from queue import Queue
from urllib.parse import unquote

from bs4 import BeautifulSoup, Comment
from bs4.element import Tag, NavigableString
from docx import Document
from pyecharts import options as opts
from pyecharts.charts import Graph
from pyecharts.options import GraphNode, GraphLink
from pyhanlp import HanLP  # 使用前导入 HanLP工具

import src.app.gol as gol
from src.IO.databaseInteraction.MSSQL import SqlServerProcessor
from src.IO.fileInteraction.FileIO import FileIO
from src.spider.WebSpider import WebSpider
from src.tableExtract.table import changeTig2Table, Table, TableItem, changeWordTable2Table
from src.tools.algorithm.exceptionCatch import except_output


class TableExtract:
    def __init__(self):
        """
        初始化函数
        """
        self.segment = HanLP.newSegment()
        # 名词属性列表
        self.attrTypeSet = frozenset(
            ["n", "nb", "nba", "nbc", "nbp", "nf", "ng", "nh", "nhd", "nhm", "ni", "nic", "nis", "nit", "nl", "nm",
             "nmc", "nn", "nnd", "nnt", "nr", "nr1", "nr2", "nrf", "nrj", "ns", "nsf", "nt", "ntc", "ntcb", "ntcf",
             "ntch", "nth", "nto", "nts", "ntu", "nx", "nz"])
        self.nowName = ''
        self.nowUrl = ''
        self.sql = SqlServerProcessor()
        self.maxSize = 200
        self.pendingQueue = Queue(self.maxSize)
        self.addInitialUrl2Queue()
        self.urlList = []
        self.url2pathDict = {}
        self.running = False
        self.url2pathDictPath = f"{gol.get_value('configurationPath')}\\url2pathDict.pkl"
        if not os.path.exists(self.url2pathDictPath):
            FileIO.writePkl(self.url2pathDictPath, self.url2pathDict)
        else:
            self.url2pathDict = FileIO.readPkl(self.url2pathDictPath)

        self.tagTable = []
        self.tableList = []
        self.staticList = []
        self.staticInnerList = []

    def addInitialUrl2Queue(self):
        """
        初始化url队列，将部分url链接
        :return:
        """
        urlList = [
            r"https://baike.baidu.com/item/%E5%AD%94%E5%AD%90/1584",
            r"https://baike.baidu.com/item/%E7%9E%BF%E9%A2%96", ]
        urlHtmlDict = {}
        DictPath = f"{gol.get_value('otherConfigurationPath')}\\urlHtmlDict.pkl"
        if not os.path.exists(DictPath):
            FileIO.writePkl(DictPath, urlHtmlDict)
        else:
            urlHtmlDict = FileIO.readPkl(DictPath)
        for url in urlList:
            if url in urlHtmlDict:
                html = urlHtmlDict[url]
            else:
                html = WebSpider.getHtml(url)
                urlHtmlDict[url] = html
            self.pendingQueue.put([url, html])
        FileIO.writePkl(DictPath, urlHtmlDict)

    @except_output()
    def start(self, maxWaitTimes=float('inf')):
        self.running = True
        print("开始抽取表格...")
        _tableDocPath = gol.get_value("tableDocPath")
        _jsonPath = gol.get_value("jsonPath")
        waitTimes = maxWaitTimes  # maxWaitTime次的等待,如果结束了，数据库中都没有数据，那么终止该程序
        while waitTimes:
            if not self.running:
                return
            self.addQueue(self.pendingQueue, 'personUrlAndHtml')
            while not self.pendingQueue.empty():
                if not self.running:
                    return
                waitTimes = maxWaitTimes
                dataTuple = self.pendingQueue.get_nowait()
                url = dataTuple[0]
                html = dataTuple[1]
                last = unquote(url.split('/')[-1])
                self.nowName = unquote(url.split('/')[-2]) if last.isdigit() else last
                self.nowUrl = url

                _tableList = self.getTable(html)  # 获取表格
                self.dealWithTableList(_tableList)  # 处理表格的list
                self.addQueue(self.pendingQueue, 'personUrlAndHtml')  # 补充队列

            time.sleep(0.2)
            waitTimes -= 1

    def dealWithTableList(self, _tableList: list):
        """
        处理Table对象的list，将这些表格中的实体和关系抽取出来，之后写下记录文件，便于之后的动态可视化处理
        将“表格、实体、关系、关系图”全都写入到本地文件中
        :param _tableList: Table的列表，该列表中每个元素都是一个Table对象
        :return:
        """
        entityListAndRelationshipList = []
        # 将当前表格深度复制一份，便于之后的删除行列的操作
        _extendTableList = deepcopy(_tableList)
        for table in _extendTableList:
            table.hrefMap[self.nowName] = self.nowUrl  # 超链接映射
            table = table.extendTable()  # 表格规整
            table.prefix = self.nowName  # 设置表格的前驱
            if table.isNormal() and table.isCorrect():  # 判断表格是否正常且正确
                if table.getUnfoldDirection() == "COL":  # 把表格全部变成横向展开的
                    table = table.flip()
                table.clearTable()  # 清理表格数据
                entity, relationship = table.extractEntityRelationship()  # 抽取实体和关系
                # 若存在实体和关系，则将其写入到数据库之中
                if len(entity) != 0 or len(relationship) != 0:
                    entityJson = json.dumps(entity, ensure_ascii=False)
                    relationshipJson = json.dumps(relationship, ensure_ascii=False)
                    entityListAndRelationshipList.append([entity, relationship])
                    self.sql.writeER2DB('entityAndRelationship', entityJson, relationshipJson)
        # 将数据记录到本地文件，方便可视化处理
        self.writeRecords(self.nowUrl, _tableList, entityListAndRelationshipList)

    def stop(self):
        """
        停止表格抽取程序
        :return:
        """
        self.running = False

    def getTable(self, _html: str) -> list:
        """
        表格定位，从html中抽取表格，并返回一个list，该list中每个元素都是一个Table对象
        :param _html:待抽取的html串
        :return:一个list，该list中每个元素都是一个Table对象
        """
        # 预处理
        _html, _soup = htmlPreTreat(_html)  # html预处理
        _tableList = self.extractNonstandardTable(_soup)  # 抽取非规范表，也就是百度百科中的“基本信息”(basic info)列表
        tagTable = _soup.find_all(name="table")  # 找到所有的Table标签

        for tag in tagTable:
            # 首先判断是否为非规范表，这种非规范表带有<table>标签，内部却是ul标签，需要当初考虑和处理
            tableList = self.extractListTable(tag)
            if len(tableList) > 0:
                _tableList.extend(tableList)
            elif self.throughHeuristicRule(tag):  # 看是否能通过启发式规则
                caption = getCaption(tag)  # 获取表格的标题
                aTable = changeTig2Table(tag, caption)  # 将标签转化为Table对象
                _tableList.append(aTable)
        return _tableList

    @except_output("启发式规则判别出错")
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
                if len(now.contents) == 1:
                    now = now.nextSibling
                if len(now.next.contents) >= 2 or len(now.contents) >= 2:
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
            for table in tables:
                if type(table) == Tag:
                    if type(table.descendants) == Tag:
                        sub_table.append(table.descendants.find_all('table'))
            scripts = tag.find_all('script')
            img = tag.find_all('img')
            thead = tag.find("thead")
            tbody = tag.find("tbody")
            if thead and tbody:
                rows = len(thead.contents) + len(tbody.contents)
                cols = len(thead.next.contents)
            else:
                ul = tag.find('ul')
                if ul:
                    rows = len(ul.contents) + 1
                    cols = len(ul.contents) - 1
                else:
                    rows = len(tag.contents)
                    now = tag.contents[0]
                    if now.name == "caption":
                        now = now.nextSibling
                    cols = len(now.contents)
            if len(hrefs) > rows * cols * 2 or len(sub_table) > 3 or len(scripts) > 1 or len(img) > rows * cols / 2:
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
            if len(tagContents) >= 2:
                # 判断前2行、前2列是否有属性
                for tagContent in tagContents[0:2]:
                    if tagContent.name == "caption":
                        continue
                    contentList = tagContent.contents
                    for content in contentList:
                        results = list(tableExtractor.segment.seg(content.text))
                        natureList = [str(result.nature) for result in results]
                        if natureList.count("n") > 0:
                            return True
                        # for result in results:
                        #     if str(result.nature) not in tableExtractor.attrTypeSet:
                        #     return False

                        # return True
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
        tableList = self.extractListTableWithRule1(tag, ruleDict)
        # tableList.extend(self.extractListTableWithRule2(tag))
        return tableList

    def extractListTableWithRule1(self, tag: Tag, ruleDict: dict) -> list:
        """
        按照规则1，从Tag抽取非规范表格（不带有table标签的表格）
        :param tag:带抽取Tag
        :param ruleDict: 规则映射字典
        :return: 抽取的表格的list
        """
        _tableList = []
        if "class" in ruleDict:
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

    def extractListTableWithRule2(self, _tag: Tag) -> list:
        """
        按照规则2，从Tag中抽取列表
        :param _tag: 带抽取标签
        :return: 抽取的表格的list
        """
        _tableList = []
        tagTable = _tag.find_all(name="table")
        for tag in tagTable:
            newTable = self.extractListTable(tag)
            if newTable is not None:
                _tableList.extend(newTable)
        return _tableList

    @staticmethod
    def extractListTable(tag: Tag) -> list:
        """
        从Tag中抽取列表
        :param tag:带抽取标签
        :return: 表格的list
        """
        tableList = []
        titleList = tag.find_all(class_='normal title-td')
        ulList = tag.find_all("ul")
        if len(titleList) == len(ulList):
            for i in range(len(titleList)):
                titleTag = titleList[i]
                dataTag = ulList[i]
                caption = titleTag.text
                liList = dataTag.contents
                cellList = []
                colSizeList = []
                rowNumber = len(liList)
                for rowIndex in range(rowNumber):
                    li = liList[rowIndex]
                    spans = li.find_all("span")
                    colIndex = 0
                    innerList = []
                    for span in spans:
                        if span.text == '▪':
                            continue
                        else:
                            text = span.text.strip()
                            href = {}
                            aList = span.find_all("a")
                            if aList:
                                for a_node in aList:
                                    if a_node.has_attr("href"):
                                        href[a_node.text] = a_node['href']

                            imgSrc = []
                            imgList = span.find_all("img")
                            for img in imgList:
                                if img.has_attr("src"):
                                    imgSrc.append(img["src"])
                            newTableItem = TableItem(text, rowIndex, 1, colIndex, 1, href, imgSrc,
                                                     tagName=span.name)
                            colIndex += 1
                            innerList.append(newTableItem)
                        colSizeList.append(colIndex)
                    cellList.append(innerList)
                newTable = Table(rowNumber, max(colSizeList), caption, table=cellList)
                tableList.append(newTable)
        return tableList

    def addQueue(self, QueueName: Queue, tableName: str):
        """
        从指定的数据库中抽取数据，补充到指定的队列
        :param QueueName: 队列名
        :param tableName: 数据表名
        :return: 无
        """
        # 队列长度小于一半，则从数据库中补充到队列
        if QueueName.qsize() < int(QueueName.maxsize / 10):
            urlAndHtml = self.sql.getUrlAndHtmlFromDB(tableName, int(QueueName.maxsize / 2))
            for url in urlAndHtml:
                QueueName.put(url)
            self.sql.deleteFromDBWithIdNum(tableName, int(QueueName.maxsize / 2))  # 删除

    def addUrlList(self, url: str):
        """
        在url列表中添加一个表格。若url链接数量大于最大列表长度，则剔除第一个url，同时删除该url对应的本地文件
        :param url: 待添加的url
        :return: 无
        """
        if len(self.urlList) >= self.maxSize:
            tempUrl = self.urlList[0]
            self.urlList = self.urlList[1:]
            if tempUrl in self.url2pathDict:
                Id = self.url2pathDict[tempUrl]
                pklFilepath = f"{gol.get_value('tablePklPath')}\\{Id}.pkl"
                htmlFilePath = f"{gol.get_value('relationChartPath')}\\{Id}.html"
                if os.path.exists(pklFilepath):
                    os.remove(pklFilepath)
                if os.path.exists(htmlFilePath):
                    os.remove(htmlFilePath)
                del self.url2pathDict[tempUrl]
        self.urlList.append(url)

    def writeRecords(self, url: str, tableList: list, entityAndRelationshipList: list):
        """
        写下记录，将表格、实体和关系均存入到一个pkl文件中
        :param url:当前的url链接
        :param tableList: 表格的list
        :param entityAndRelationshipList:实体和关系的list
        :return: 无
        """

        def getEListAndRList(ERLIst: list):
            entityList_ = []
            relationshipList_ = []
            for ER in ERLIst:
                entityList_.extend(ER[0])
                relationshipList_.extend(ER[1])
            return entityList_, relationshipList_

        if len(tableList) != 0:
            url = unquote(url)
            self.url2pathDict = FileIO.readPkl(self.url2pathDictPath)
            Id = self.nowName + time.strftime('%Y_%H_%M_%S', time.localtime()) + str(random.randint(0, 100))
            tableListPath = f"{gol.get_value('tablePklPath')}\\{Id}.pkl"
            relationChartPath = f"{gol.get_value('relationChartPath')}\\{Id}.html"
            self.url2pathDict[url] = Id
            FileIO.writePkl(self.url2pathDictPath, self.url2pathDict)
            entityList, relationshipList = getEListAndRList(entityAndRelationshipList)
            writeER2Chart(entityList, relationshipList, relationChartPath)
            FileIO.writePkl(tableListPath, [tableList, entityAndRelationshipList])
            self.addUrlList(url)


def getCaption(_tag: Tag):
    """
    提取标签中的表格标题 和 标题前缀
    :param _tag:输入的标签
    :return:表格标题 和 标题前缀
    """
    _caption = "未命名表格"
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
                    _caption = title.contents[1]
                elif len(title.contents) == 1:
                    if isinstance(title.contents[0], NavigableString):
                        _caption = title.contents[0]
                    else:
                        _caption = title.contents[0].text
                    return str(_caption)
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
            _previous = _previous.previous_sibling
            if _previous:
                title = _previous.find(attrs={"class": re.compile(r"^.*title.*$")})
                if title:
                    if len(title.contents) == 2:
                        _caption = title.contents[1]
                    elif len(title.contents) == 1:
                        if isinstance(title.contents[0], NavigableString):
                            _caption = title.contents[0]
                        else:
                            _caption = title.contents[0].text

    # _caption = re.sub(u"\(.?\)|\\（.*?）|\\{.*?}|\\[.*?]|\\【.*?】||\\<.*?\\>", "", _caption)  # 去除括号
    return str(_caption)


def extractWordTable(filename: str) -> list:
    """
    将word文档中的表格转化为Table类
    :param filename: 文件名，人名
    :return:
    """
    try:
        doc = Document(filename)
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


def writeER2Chart(entityList: list, relationshipList: list, filepath: str, title=''):
    """
    将实体和关系转化为关系图的html文件
    :param entityList: 实体列表
    :param relationshipList: 关系列表
    :param filepath: html文件的绝对路径
    :param title: 当前关系图的标题
    :return: 无
    """

    def change2NodeAndLink(entityList_: list, relationshipList_: list):
        def changeEList2NodeList(entityList__: list, url2NameDict__=None) -> list:
            """
            将实体列表转化为节点列表
            :param entityList__: 实体列表
            :param url2NameDict__: url:人名 的映射字典，用于去除重复
            :return: 节点列表
            """

            def dumpDict(ADict: dict) -> str:
                string = ""
                for key in ADict:
                    string += f"{key}:{ADict[key]}\n"
                return string

            if url2NameDict__ is None:
                url2NameDict__ = {}
            NameSet = set()
            NodeList = []

            for entity in entityList__:
                name = entity[0][0]
                url = entity[0][1]
                propertyDict = entity[1]
                if len(url.strip()) != 0 and url in url2NameDict__:
                    continue
                if name in NameSet:
                    continue
                else:
                    NameSet.add(name)
                newNode = GraphNode(name=name, symbol_size=30, value=[dumpDict(propertyDict)])
                NodeList.append(newNode)
                url2NameDict__[url] = name
            return NodeList

        def changeRList2linkList(relationshipList_: list, url2NameDict_=None) -> list:
            """
            将关系列表转化为链接链表
            :param relationshipList_:关系列表
            :param url2NameDict_: url:人名 的映射字典，用于去除重复
            :return: 链接列表
            """
            if url2NameDict_ is None:
                url2NameDict_ = {}
            linkList = []
            for relationship in relationshipList_:
                name1 = str(relationship[0][0])
                url1 = str(relationship[0][1])
                R = relationship[1]
                name2 = str(relationship[2][0])
                url2 = str(relationship[2][1])
                if len(url1.strip()) != 0 and url1 in url2NameDict_:
                    name1 = url2NameDict_[url1]
                if len(url2.strip()) != 0 and url2 in url2NameDict_:
                    name2 = url2NameDict_[url2]
                newLink = GraphLink(source=name1, target=name2, value=R)
                linkList.append(newLink)
                url2NameDict_[url1] = name1
                url2NameDict_[url2] = name2
            return linkList

        url2NameDict = {}
        __nodes_data = changeEList2NodeList(entityList_, url2NameDict)
        __links_data = changeRList2linkList(relationshipList_, url2NameDict)
        return __nodes_data, __links_data

    def writeNodeAndLink2Html(nodes_data_: list, links_data_: list, filepath_: str, title_=''):
        """
        将节点和链接写入到html文件中，形成关系图
        :param nodes_data_: 节点列表
        :param links_data_: 链接列表
        :param filepath_: html文件的绝对路径
        :param title_: 关系图的标题
        :return:无
        """
        c = (
            Graph()
                .add(
                "",
                nodes_data_,
                links_data_,
                repulsion=1000,
                edge_label=opts.LabelOpts(
                    is_show=True, position="middle", formatter="{c}"
                ),
            )
                .set_global_opts(
                title_opts=opts.TitleOpts(title=title_)
            )
                .render(filepath_)
        )

    nodes_data, links_data = change2NodeAndLink(entityList, relationshipList)
    writeNodeAndLink2Html(nodes_data, links_data, filepath, title_=title)
