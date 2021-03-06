import os
import random
import threading
import time
from queue import Queue
from time import sleep
from urllib.error import URLError, HTTPError
from urllib.parse import unquote

import requests.exceptions
from pybloom_live import ScalableBloomFilter
from requests.adapters import HTTPAdapter

import src.app.gol as gol
from src.IO.databaseInteraction.MSSQL import SqlServerProcessor
from src.IO.fileInteraction.FileIO import FileIO
from src.spider.UrlExtractor import UrlExtractor
from src.spider.UserAgent import USER_AGENTS
from src.tools.algorithm.exceptionCatch import except_output


class WebSpider:
    def __init__(self):
        """
        爬虫初始化函数
        """
        self.queueLength = 5000  # 队列长度
        self.pendingQueue = Queue(self.queueLength)  # 待处理队列
        self.waitWriteQueue = Queue(self.queueLength)  # 等待写入的队列
        self.projectPath = gol.get_value("projectPath")
        self.spiderFilePath = gol.get_value("spiderFilePath")
        self.SeedPath = os.path.join(self.spiderFilePath, 'PersonUrlSeedLink.txt')  # 种子文件路径
        self.urlBloomPath = os.path.join(self.spiderFilePath, 'urlBloom.pkl')  # url布隆过滤器所在路径
        self.urlBloom = ScalableBloomFilter(initial_capacity=100, error_rate=0.001)  # 自动扩容的布隆过滤器
        self.maxCount = 10000  # 爬取的最大次数
        self.spiderCount = 1  # 爬虫爬取次数
        self.writeUrlCount = 1  # 写入Url次数
        self.seedQueue = Queue()  # 种子队列
        self.running = False  # 是否正在运行
        self.sql = SqlServerProcessor()
        if not os.path.exists(self.urlBloomPath):
            FileIO.writePkl(self.urlBloomPath, self.urlBloom)
        else:
            self.urlBloom = FileIO.readPkl(self.urlBloomPath)

    def readSeed(self, SeedPath: str) -> Queue:
        """
        读取种子文件到待爬队列
        :return:返回种子队列
        """
        with open(SeedPath, mode="r", encoding='utf-8') as f:
            _url = f.readline()
            while _url:
                if not self.seedQueue.full():
                    self.seedQueue.put(_url)
                else:
                    return self.seedQueue
                _url = f.readline()
            return self.seedQueue

    @staticmethod
    def getHtml(url, timeout=1.0, uer_agent=USER_AGENTS) -> str:
        """
        根据url获取html
        :param url: 传入的url
        :param timeout: 获取url时等待的最长时间
        :param uer_agent: 获取url时使用的代理头
        :return: 返回的html
        """
        try:
            sess = requests.Session()
            sess.mount('http://', HTTPAdapter(max_retries=3))
            sess.mount('https://', HTTPAdapter(max_retries=3))
            sess.keep_alive = False  # 关闭多余连接
            response = requests.get(url=url,
                                    headers={'User-Agent': random.choice(uer_agent),
                                             'Connection': 'close'},
                                    timeout=timeout,
                                    )
            if response.status_code == 200:
                return response.content.decode('utf-8')
        except requests.exceptions.ConnectTimeout as e:
            print(e)
        except requests.exceptions.Timeout as e:
            print(e)
        except HTTPError as e:
            print(e)
        except URLError as e:
            print(e)
        except Exception as e:
            print(e)

    def start(self, threadsNum: int = 1, maxCount=10000, IsDealWithSeed=True):
        """
        开启爬虫
        :param IsDealWithSeed: 是否处理种子文件
        :param threadsNum:线程数
        :param maxCount:最大爬取次数
        :return:无
        """
        self.running = True
        requests.DEFAULT_RETRIES = 0  # 重试连接次数
        s = requests.session()
        s.keep_alive = False  # 关闭多余连接
        self.maxCount = maxCount
        # 读取种子链接到待爬队列
        if IsDealWithSeed:
            self.dealWithSeed()
        for ID in range(1, threadsNum + 1):
            threading.Thread(target=self.startSpider, args=(ID,)).start()
        threading.Thread(target=self.writeQueue2Database).start()
        threading.Thread(target=self.dealWithUselessUrl).start()

    def stop(self):
        """
        爬虫停止运行
        :return:
        """
        self.running = False

    def dealWithSeed(self):
        """
        处理种子url链接
        :return:
        """
        self.readSeed(self.SeedPath)
        _urlExtractor = UrlExtractor()
        while not self.seedQueue.empty():
            _url = self.seedQueue.get()
            print(f">>>>正在爬取种子url:{unquote(_url)}")
            _html = self.getHtml(url=_url, timeout=1)
            _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
            if len(_usefulUrlSet):
                differenceUrlList = self.getDifferenceFromBloom(_usefulUrlSet)
                self.sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceUrlList)
            if len(_uselessUrlSet):
                possibleUrl = self.__getPossibleUrl(_uselessUrlSet)
                differenceUrlList = self.getDifferenceFromBloom(possibleUrl)
                self.sql.writeUrlToDB(tableName="uselessUrl", urlList=differenceUrlList)

    @except_output()
    def startSpider(self, ID: int):
        """
        开启单线程爬虫
        :param ID:当前爬虫的ID
        :return: 无
        """
        # url抽取器
        _urlExtractor = UrlExtractor()
        # BFS 广度优先遍历
        while True:
            if not self.running:  # 运行状态检查
                return
            self.addQueue(self.pendingQueue, "pendingUrl")
            while not self.pendingQueue.empty():
                if not self.running:
                    return
                _url = str(self.pendingQueue.get())
                print(f">>>>爬虫<{ID}>:正在抽取第<{self.spiderCount}>条url：{unquote(_url)}")
                if self.spiderCount >= self.maxCount:
                    return
                self.spiderCount += 1
                _html = self.getHtml(url=_url, timeout=1)
                # 则从数据库中补充url到队列
                self.addQueue(QueueName=self.pendingQueue, tableName='pendingUrl')
                if _html:
                    self.sql.writeUrlAndHtmlToDB(tableName="personUrlAndHtml", url=_url, _html=_html)
                    # 从html中抽取url
                    _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
                    # 当前url和html写入到数据库中
                    if len(_usefulUrlSet):
                        # 获取不在过滤器中的差异url列表,并将差异url列表写入布隆过滤器中
                        differenceUrlList = self.getDifferenceFromBloom(_usefulUrlSet)
                        # 差集写入数据库
                        # self.sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceUrlList)
                        for url in differenceUrlList:
                            self.waitWriteQueue.put(url)
                    if len(_uselessUrlSet):
                        possibleUrl = self.__getPossibleUrl(_uselessUrlSet)
                        differenceUrlList = self.getDifferenceFromBloom(possibleUrl)
                        self.sql.writeUrlToDB(tableName="uselessUrl", urlList=differenceUrlList)
                    FileIO.writePkl(self.urlBloomPath, self.urlBloom)  # 布隆过滤器

                while self.pendingQueue.empty():
                    if not self.running:
                        return
                    self.addQueue(self.pendingQueue, "pendingUrl")

    def writeQueue2Database(self, tableName: str = "pendingUrl"):
        """
        将队列中的URL写入到指定的数据表之中，并清空队列
        :param tableName:数据表
        :return: 无
        """
        while True:
            if not self.waitWriteQueue.empty():
                urlList = list(self.waitWriteQueue.queue)
                self.waitWriteQueue.queue.clear()
                self.sql.writeUrlToDB(tableName, urlList)
            time.sleep(0.3)

    @staticmethod
    def __getPossibleUrl(urlSet: set):
        """
        从url集合中获取与主题可能有关的url链接集合
        :param urlSet:url集合
        :return: 与主题可能有关的url链接集合
        """
        newSet = set()
        for url in urlSet:
            url = str(url)
            if url.startswith(r"https://baike.baidu.com/item/"):
                if "." in url.split('/')[-1]:
                    continue
                newSet.add(url)
        return newSet

    def dealWithUselessUrl(self, maxWaitTimes=10000, maxQueueSize=5000):
        """
        开始处理无效的（与主题无关的）的url链接
        :param maxWaitTimes: 处理的最大次数
        :param maxQueueSize: 缓冲队列的最大长度
        :return: 无
        """
        print("开始处理无用url...")
        waitTimes = maxWaitTimes
        uselessUrlQueue = Queue(maxsize=maxQueueSize)
        _urlExtractor = UrlExtractor()
        while waitTimes:
            if not self.running:
                return
            self.addQueue(QueueName=uselessUrlQueue, tableName='uselessUrl')
            while not uselessUrlQueue.empty():
                _url = uselessUrlQueue.get_nowait()
                _html = self.getHtml(url=_url, timeout=1)
                if _html:
                    _usefulUrlSet, _uselessUrlSet = _urlExtractor.extractUrl(_html)
                    if len(_usefulUrlSet):
                        differenceList = self.getDifferenceFromBloom(_usefulUrlSet)
                        self.sql.writeUrlToDB(tableName="pendingUrl", urlList=differenceList)
                        # if len(differenceList):
                        #     print(f">>>>已找到的有用链接个数：<{len(differenceList)}>")
                    if len(_uselessUrlSet):
                        possibleUrl = self.__getPossibleUrl(_uselessUrlSet)
                        differenceList = self.getDifferenceFromBloom(possibleUrl)
                        self.sql.writeUrlToDB(tableName="uselessUrl", urlList=differenceList)
                self.addQueue(QueueName=uselessUrlQueue, tableName='uselessUrl')
            sleep(0.1)
            waitTimes -= 1

    def getDifferenceFromBloom(self, urlSet: set) -> list:
        """
        从布隆过滤器中获取当前url集合的差集列表
        :param urlSet: url集合
        :return:差集列表
        """
        differenceUrlList = []
        for url in urlSet:
            if url not in self.urlBloom:
                self.urlBloom.add(url)
                differenceUrlList.append(url)
        return differenceUrlList

    def addQueue(self, QueueName: Queue, tableName: str):
        """
        从指定的数据表中读取数据补充到指定的队列中
        :param QueueName: 指定的队列
        :param tableName: 指定的数据表
        :return: 无
        """
        # 队列长度小于1/10，则从数据库中补充到队列
        if QueueName.qsize() < int(QueueName.maxsize / 10):
            urls = self.sql.getUrlFromDB(tableName, int(QueueName.maxsize / 2))
            if urls is None or len(urls) == 0:
                pass
            else:
                for url in urls:
                    QueueName.put(url)
                self.sql.deleteFromDBWithIdNum(tableName, int(QueueName.maxsize / 2))  # 删除

    def __del__(self):
        """
        析构时写回布隆过滤器
        :return:
        """
        FileIO.writePkl(self.urlBloomPath, self.urlBloom)
