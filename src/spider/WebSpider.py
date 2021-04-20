import os
import random
import threading
from queue import Queue
from time import sleep

import requests.exceptions
from pybloom_live import ScalableBloomFilter
from urllib.parse import unquote
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
        self.sql = SqlServerProcessor()
        if not os.path.exists(self.urlBloomPath):
            FileIO.writePkl(self.urlBloomPath, self.urlBloom)
        else:
            self.urlBloom = FileIO.readPkl(self.urlBloomPath)

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
        # self.sql.clearAllTable()  # 重置当前所有
        self.maxCount = maxCount
        self.startSpider()

    def startSpider(self):
        # url抽取器
        _urlExtractor = UrlExtractor()
        # 读取种子链接到待爬队列
        SeedNum = self.readSeed(self.SeedPath)
        threading.Thread(target=self.dealWithUselessUrl).start()
        if SeedNum >= 0:
            # 计数
            count = 1
            # BFS 广度优先遍历
            while not self.pendingQueue.empty():
                _url = self.pendingQueue.get_nowait()
                SeedNum -= 1
                print(f"正在抽取的url：{unquote(_url)}")
                _html = self.getHtml(url=_url, timeout=1)
                # 队列长度小于一半，则从数据库中补充到队列
                self.addQueue(QueueName=self.pendingQueue, tableName='pendingUrl')
                if _html:
                    # 从html中抽取url
                    _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
                    if len(_usefulUrlSet):
                        # 获取不在过滤器中的差异url列表,并将差异url列表写入布隆过滤器中
                        differenceUrlList = self.getDifferenceFromBloom(_usefulUrlSet)
                        # 差集写入数据库
                        self.sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceUrlList)
                        # 当前url：html写入到数据库中
                        if SeedNum < 0:
                            self.sql.writeUrlAndHtmlToDB(tableName="personUrlAndHtml", url=_url, _html=_html)
                            SeedNum = max(SeedNum, -1)

                    if len(_uselessUrlSet):
                        differenceUrlList = self.getDifferenceFromBloom(_uselessUrlSet)
                        self.sql.writeUrlToDB(tableName="uselessUrl", urlList=differenceUrlList)
                    FileIO.writePkl(self.urlBloomPath, self.urlBloom)

                if count == self.maxCount:
                    return
                else:
                    print(f"当前次数count:{count}")
                    count = count + 1

    def dealWithUselessUrl(self, maxWaitTimes=10000, maxQueueSize=500):
        waitTimes = maxWaitTimes
        uselessUrlQueue = Queue(maxsize=maxQueueSize)
        _urlExtractor = UrlExtractor()
        while waitTimes:
            self.addQueue(QueueName=uselessUrlQueue, tableName='uselessUrl')
            while not uselessUrlQueue.empty():
                _url = uselessUrlQueue.get_nowait()
                _html = self.getHtml(url=_url, timeout=1)
                if _html:
                    _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
                    if len(_usefulUrlSet):
                        differenceList = self.getDifferenceFromBloom(_usefulUrlSet)
                        self.sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceList)
                    if len(_uselessUrlSet):
                        differenceList = self.getDifferenceFromBloom(_uselessUrlSet)
                        self.sql.writeUrlToDB(tableName="uselessUrl", urlList=differenceList)
                self.addQueue(QueueName=uselessUrlQueue, tableName='uselessUrl')
            sleep(1.0)
            waitTimes -= 1

    def getDifferenceFromBloom(self, urlSet: set) -> list:
        """
        从布隆过滤器中获取当前url集合的差集列表
        :param urlSet: url集合
        :return:
        """
        differenceUrlList = []
        for url in urlSet:
            if url not in self.urlBloom:
                self.urlBloom.add(url)
                differenceUrlList.append(url)
        return differenceUrlList

    def addQueue(self, QueueName: Queue, tableName: str):
        # 队列长度小于一半，则从数据库中补充到队列
        if QueueName.qsize() < int(QueueName.maxsize / 2):
            for url in self.sql.getUrlFromDB(tableName, int(QueueName.maxsize / 2)):
                QueueName.put_nowait(url)
            self.sql.deleteFromDBWithIdNum(tableName, int(QueueName.maxsize / 2))  # 删除

    def __del__(self):
        # 析构时写回布隆过滤器
        FileIO.writePkl(self.urlBloomPath, self.urlBloom)


def stopSpider(self):
    # TODO:停止爬虫所在线程
    pass
