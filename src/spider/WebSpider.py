import os
import random
from queue import Queue

import requests.exceptions
from pybloom_live import ScalableBloomFilter

import src.app.gol as gol
from src.IO.fileInteraction.FileIO import FileIO
from src.spider.UrlExtractor import UrlExtractor
from src.spider.UserAgent import USER_AGENTS
from src.tools.algorithm.exceptionCatch import except_output
from src.IO.databaseInteraction.MSSQL import SqlServerProcessor


class WebSpider:
    def __init__(self):
        self.queueLength = 500  # 队列长度
        self.pendingQueue = Queue(self.queueLength)  # 待处理队列
        self.projectPath = gol.get_value("projectPath")
        self.spiderFilePath = gol.get_value("spiderFilePath")
        self.SeedPath = os.path.join(self.spiderFilePath, 'PersonUrlSeedLink.txt')  # 种子文件路径
        self.urlBloomPath = os.path.join(self.spiderFilePath, 'urlBloom.pkl')  # url布隆过滤器所在路径
        self.urlBloom = ScalableBloomFilter(initial_capacity=100, error_rate=0.001)  # 自动扩容的布隆过滤器
        self.maxCount = 10000  # 爬取的最大次数
        if not os.path.exists(self.urlBloomPath):
            FileIO.writePkl(self.urlBloomPath, self.urlBloom)

    def readSeed(self, SeedPath: str) -> int:
        """
        读取种子文件到待爬队列
        :return:返回值为整数n，表示读取了n个种子到待爬队列
        """
        with open(SeedPath, mode="r", encoding='utf-8') as f:
            _count = 0
            _url = f.readline()
            while _url:
                if not self.pendingQueue.full():
                    self.pendingQueue.put_nowait(_url)
                    _count += 1
                else:
                    return _count
                _url = f.readline()
            return _count

    @staticmethod
    @except_output()
    def getHtml(url, timeout=1.0, uer_agent=USER_AGENTS) -> str:
        """
        根据url获取html
        :param url: 传入的url
        :param timeout: 获取url时等待的最长时间
        :param uer_agent: 获取url时使用的代理头
        :return: 返回的html
        """
        response = requests.get(url=url, headers={'User-Agent': random.choice(uer_agent)}, timeout=timeout)
        if response.status_code == 200:
            return response.content.decode('utf-8')

    def start(self, maxCount=10000):
        """
        开启爬虫
        :param maxCount:最大爬取次数
        :return:
        """
        self.maxCount = maxCount
        # url抽取器
        _urlExtractor = UrlExtractor()
        # 读取种子链接到待爬队列
        SeedNum = self.readSeed(self.SeedPath)
        sql = SqlServerProcessor()
        # sql.clearAllTable()  # 重置当前所有
        if SeedNum >= 0:
            # 计数
            count = 1
            # BFS 广度优先遍历
            while not self.pendingQueue.empty():
                _url = self.pendingQueue.get_nowait()
                SeedNum -= 1
                print(f"正在抽取的url：{_url}")
                _html = self.getHtml(url=_url, timeout=1)

                # 队列长度小于一半，则从数据库中补充到队列
                if self.pendingQueue.qsize() < int(self.pendingQueue.maxsize / 2):
                    for url in sql.getUrlFromDB(tableName="pendingUrl",
                                                limitNum=int(self.pendingQueue.maxsize / 2)):
                        self.pendingQueue.put_nowait(url)
                    sql.deleteFromDBWithIdNum("pendingUrl", int(self.pendingQueue.maxsize / 2))  # 删除

                if _html:
                    # 从html中抽取url
                    _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
                    if len(_usefulUrlSet):
                        # 读取布隆过滤器
                        self.urlBloom = FileIO.readPkl(self.urlBloomPath)
                        # 获取不在过滤器中的差异url列表,并将差异url列表写入布隆过滤器中
                        differenceUrlList = []
                        for url in _usefulUrlSet:
                            if url not in self.urlBloom:
                                self.urlBloom.add(url)
                                differenceUrlList.append(url)
                        FileIO.writePkl(self.urlBloomPath, self.urlBloom)
                        # 差集写入数据库
                        sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceUrlList)
                        # 当前url：html写入到数据库中
                        if SeedNum < 0:
                            sql.writeUrlAndHtmlToDB(tableName="personUrlAndHtml", url=_url, _html=_html)
                            SeedNum = max(SeedNum, -1)

                if count == self.maxCount:
                    return
                else:
                    print(f"当前次数count:{count}")
                    count = count + 1


def stopSpider(self):
    # TODO:停止爬虫所在线程
    pass
