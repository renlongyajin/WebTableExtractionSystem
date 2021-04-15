import sqlite3
import os
from tools.algorithm.changeCode import encode, decode


class SqlForSpider:
    def __init__(self):
        self.dbPath = r'E:\SQL\Sqlite\SqliteDatabase\PendingQueue.db'

    @staticmethod
    def writeUrlToDB(tableName: str, urlList: list) -> bool:
        dbPath = r'E:\SQL\Sqlite\SqliteDatabase\PendingQueue.db'
        if not len(urlList):
            return False
        for i in range(len(urlList)):
            urlList[i] = "('" + str(urlList[i]) + "')"
        url = ",".join(urlList)
        with sqlite3.connect(dbPath) as con:
            c = con.cursor()
            sql = f"INSERT INTO {tableName}(url) VALUES {url}"
            res = c.execute(sql)
            return True

    @staticmethod
    def getUrlFromDB(tableName: str, limitNum: int) -> list:
        dbPath = r'E:\SQL\Sqlite\SqliteDatabase\PendingQueue.db'
        with sqlite3.connect(dbPath) as con:
            c = con.cursor()
            urls = c.execute(f"SELECT url FROM {tableName} limit {limitNum}")  # 先获取
            res = []
            for url in urls:
                res.append(url[0])
            c.execute(f"delete from {tableName} where ID in(select ID from {tableName} limit {limitNum})")  # 再删除
            con.commit()
            return res

    @staticmethod
    def writeHtmlToDB(tableName: str, url: str, html: str):
        dbPath = r'E:\SQL\Sqlite\SqliteDatabase\PendingQueue.db'
        if not len(html):
            return False
        with sqlite3.connect(dbPath) as con:
            c = con.cursor()
            htmlCode = encode(html)
            sql = f'INSERT INTO {tableName}(url,html) VALUES ("{url}","{htmlCode}")'
            res = c.execute(sql)
            return True

    @staticmethod
    def getHtmlFromDB(tableName: str, limitNum: int) -> list:
        dbPath = r'E:\SQL\Sqlite\SqliteDatabase\PendingQueue.db'
        with sqlite3.connect(dbPath) as con:
            c = con.cursor()
            allData = c.execute(f"SELECT url,html FROM {tableName} limit {limitNum}")  # 先获取
            res = []
            for data in allData:
                res.append((data[0], decode(data[1])))
            c.execute(f"delete from {tableName} where ID in(select ID from {tableName} limit {limitNum})")  # 再删除
            con.commit()
            return res
