import html
import os

from py2neo import Graph
from pybloom_live import ScalableBloomFilter

from src.IO.fileInteraction.FileIO import FileIO
from src.app import gol
import pymssql

from src.tools.algorithm.exceptionCatch import except_output

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
[html] NTEXT NOT NULL
)

CREATE TABLE entityAndRelationship(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[entity] NTEXT,
[relationship] NTEXT
)

GO
"""


class SqlServerProcessor:
    def __init__(self, server: str = r"192.168.43.37", user=r"humenglong", password: str = "E=Mc2HMLZAZKN233",
                 database: str = "WebTable"):
        self.server = server  # 数据库服务器名称或IP
        self.user = user  # 用户名
        self.password = password  # 密码
        self.database = database  # 数据库名称

    @except_output()
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
                # raise ex
            finally:
                conn.close()

    @except_output()
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
            # raise ex
        finally:
            conn.close()
            return res

    @except_output()
    def deleteFromDBWithIdNum(self, tableName: str, limitNum: int):
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        try:
            sql = f"DELETE {tableName} WHERE ID IN (SELECT TOP {limitNum} ID FROM {tableName})"
            cursor.execute(sql)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            # raise ex
        finally:
            conn.close()

    @except_output()
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
            # raise ex
        finally:
            conn.close()
            return res

    @except_output()
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
                # raise ex
            finally:
                conn.close()

    @except_output()
    def writeER2DB(self, tableName: str, entityJson: str, relationshipJson: str):
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        sql = f"INSERT INTO {tableName}(entity,relationship) VALUES ('{entityJson}','{relationshipJson}')"
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            print(ex)
            # raise ex
        finally:
            conn.close()

    @except_output()
    def readERFromDB(self, tableName: str, limitNum: int) -> list:
        conn = pymssql.connect(self.server, self.user, self.password, self.database, charset='utf8')
        cursor = conn.cursor()
        res = []
        try:
            sql = f"SELECT entity,relationship FROM {tableName} WHERE ID IN (SELECT TOP {limitNum} ID FROM {tableName})"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for item in rows:
                res.append([item[0], item[1]])
        except Exception as ex:
            conn.rollback()
            # raise ex
        finally:
            conn.close()
            return res

    @except_output()
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
        sql3 = "DELETE FROM entityAndRelationship"
        try:
            cursor.execute(sql1)
            cursor.execute(sql2)
            cursor.execute(sql3)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            # raise ex
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

        # 删除知识图谱
        graph = Graph("http://localhost:7474", username="neo4j", password="h132271350570")  # 这里填自己的信息
        graph.delete_all()  # 将之前的图  全部删除
