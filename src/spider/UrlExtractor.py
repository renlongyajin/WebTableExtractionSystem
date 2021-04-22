from urllib.parse import unquote

from bs4 import BeautifulSoup
import re
from pyhanlp import HanLP  # 使用前导入 HanLP工具


class UrlExtractor:
    def __init__(self,
                 HeadLink=r'https://baike.baidu.com',
                 DiscriminantHead=r'/item/',
                 RelevanceThreshold=0.8):
        self.HeadLink = HeadLink  # 链接头，用以 根据相对路径 和 链接头 合并出需要的url
        self.DiscriminantHead = DiscriminantHead  # 判别头，用以判别是否是百度百科里面的一项
        self.RelevanceThreshold = RelevanceThreshold  # 相关度阈值，当计算所得的相关度超过阈值时，可以认为链接具有相关性
        self.segment = HanLP.newSegment().enableNameRecognize(True)  # 构建人名识别器

    def extractUrl(self, _html: str) -> (set, set):
        """
        从html中抽取url链接，返回值为一个包含这些url的集合
        :param _html: 输入的html
        :return: 包含这些url的集合
        """
        # 构造DOM树
        soup = BeautifulSoup(_html, 'lxml')
        tags = soup.find_all('a')
        _url_set = set()
        _useless_url_set = set()
        for tag in tags:
            if tag:
                href = tag.get('href')
                if href:
                    href = href.strip()
                    # 若当前链接与待求问题相关，则加入集合
                    if self.IsRelevant(href):
                        _url_set.add(self.HeadLink + href)
                    else:
                        _useless_url_set.add(self.HeadLink + href)
        return _url_set, _useless_url_set

    def CalculatingCorrelation(self, url: str) -> float:
        """
        计算相关度
        :param url:输入url
        :return: 相关度
        """
        if url.startswith(self.DiscriminantHead):
            url = url.lstrip(self.DiscriminantHead)
            if len(url.split('/')) >= 2:
                last = url.split('/')[-1]
                name = url.split('/')[-2] if last.isdigit() else last
            else:
                name = url
            name = unquote(name)
            # 对text文本进行人名识别
            result = self.segment.seg(name)
            if len(result) == 1:
                for item in result:
                    if str(item.nature) == "nr":
                        return 1.0
        return 0.0

    def IsRelevant(self, url: str) -> bool:
        """
        # 判断当前链接是否是相关链接
        :param url:
        :return:
        """
        if self.CalculatingCorrelation(url) >= self.RelevanceThreshold:
            return True
        else:
            return False
