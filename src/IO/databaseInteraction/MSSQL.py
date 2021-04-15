import html
import os

from pybloom_live import ScalableBloomFilter

from src.IO.fileInteraction.FileIO import FileIO
from src.app import gol
import pymssql

"""
需要先创建数据库，在sqlserver中输入以下语句：
CREATE DATABASE WebTable;
GO
use Webtable;
CREATE TABLE pendingUrl(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[url] TEXT NOT NULL
)

CREATE TABLE personUrlAndHtml(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[url] TEXT NOT NULL,
[html] TEXT NOT NULL
)
GO
"""


class SqlServerForSpider:
    def __init__(self, server: str = r"192.168.43.37", user=r"humenglong", password: str = "E=Mc2HMLZAZKN233",
                 database: str = "WebTable"):
        self.server = server  # 数据库服务器名称或IP
        self.user = user  # 用户名
        self.password = password  # 密码
        self.database = database  # 数据库名称

    def writeUrlToDB(self, tableName: str, urlList: list):
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        if len(urlList):
            for i in range(len(urlList)):
                urlList[i] = "('" + str(urlList[i]) + "')"
            url = ",".join(urlList)
            sql = f"INSERT INTO {tableName}(url) VALUES {url}"
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as ex:
                conn.rollback()
                raise ex
            finally:
                conn.close()

    def getUrlFromDB(self, tableName: str, limitNum: int) -> list:
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        res = []
        try:
            sql = f"SELECT url FROM {tableName} WHERE ID IN (SELECT TOP {limitNum} ID FROM {tableName})"
            cursor.execute(sql)
            urls = cursor.fetchall()
            for url in urls:
                res.append(url[0])
        except Exception as ex:
            conn.rollback()
            raise ex
        finally:
            conn.close()
            return res

    def deleteFromDBWithIdNum(self, tableName: str, limitNum: int):
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        try:
            sql = f"DELETE {tableName} WHERE ID IN (SELECT TOP {limitNum} ID FROM {tableName})"
            cursor.execute(sql)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            raise ex
        finally:
            conn.close()

    def getUrlAndHtmlFromDB(self, tableName: str, limitNum: int) -> list:
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        res = []
        try:
            sql = f"SELECT url,html FROM {tableName} WHERE ID IN (SELECT TOP {limitNum} ID FROM {tableName})"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for item in rows:
                res.append([item[0], html.unescape(item[1])])
        except Exception as ex:
            conn.rollback()
            raise ex
        finally:
            conn.close()
            return res

    def writeUrlAndHtmlToDB(self, tableName: str, url: str, _html: str):
        if len(_html):
            conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
            cursor = conn.cursor()
            htmlCode = html.escape(_html)
            sql = f"INSERT INTO {tableName}(url,html) VALUES ('{url}','{htmlCode}')"
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as ex:
                conn.rollback()
                raise ex
            finally:
                conn.close()

    def clearAllTable(self):
        """
        该函数是为了方便重置,以测试程序效果。
        1.清除 pendingUrl 和 personUrlAndHtml 两张表内的全部内容
        2.重置了url记录的布隆过滤器文件
        3.删除 实体三元组csv文件，关系三元组csv文件
        4.删除 未抽取三元组的表格的docx文件
        :return:
        """
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        sql1 = "DELETE FROM pendingUrl"
        sql2 = "DELETE FROM personUrlAndHtml"
        try:
            cursor.execute(sql1)
            cursor.execute(sql2)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            raise ex
        finally:
            conn.close()

        # 为了方便查看效果，重置了url记录的布隆过滤器文件
        spiderFilePath = gol.get_value('spiderFilePath')
        urlBloomPath = os.path.join(spiderFilePath, 'urlBloom.pkl')  # url布隆过滤器所在路径
        urlBloom = ScalableBloomFilter(initial_capacity=100, error_rate=0.001)  # 自动扩容的布隆过滤器
        FileIO.writePkl(urlBloomPath, urlBloom)  # 重置

        # 删除 实体三元组csv文件，关系三元组csv文件
        entityAndRelationshipPath = gol.get_value('entityAndRelationshipPath')
        entityPath = os.path.join(entityAndRelationshipPath, 'entity.csv')
        relationshipPath = os.path.join(entityAndRelationshipPath, 'relationship.csv')
        if os.path.exists(entityPath):
            os.remove(entityPath)
        if os.path.exists(relationshipPath):
            os.remove(relationshipPath)

        entityPath = f"{entityAndRelationshipPath}\\entity.json"
        if os.path.exists(entityPath):
            os.remove(entityPath)

        # 删除 未抽取三元组的表格的docx文件
        otherTablePath = f"{gol.get_value('tableDocPath')}\\未抽取三元组的表格.docx"
        if os.path.exists(otherTablePath):
            os.remove(otherTablePath)
