# -*- coding:utf-8 -*-
import os
import sys
import threading

import src.app.gol as gol
from src.knowledgeStorage.peronGraph import PersonGraph
from src.spider.Bayes import Bayes
from src.spider.WebSpider import WebSpider
from src.tableExtract.tableExtractor import TableExtract


def initial():
    sys.path.append('../')  # 加入父目录所在路径
    gol.init()  # 先必须在主模块初始化（只在Main模块需要一次即可）
    projectPath = os.path.abspath(os.path.dirname(__file__))  # 当前项目所在路径
    fileRootPath = os.path.join(projectPath, r"file")  # 文件根目录
    spiderFilePath = os.path.join(fileRootPath, r'spider')  # 爬虫的文件所在路径
    tableDocPath = os.path.join(fileRootPath, r'tableDoc')  # 表格Doc路径
    tablePklPath = os.path.join(fileRootPath, r'tablePkl')  # 表格pkl路径
    imagesPath = os.path.join(fileRootPath, r'images')  # 图片所在路径
    TriadPath = os.path.join(fileRootPath, r'Triad')  # 三元组文件路径
    configurationPath = os.path.join(fileRootPath, r'configuration')  # 设置文件路径
    BayesFilePath = os.path.join(fileRootPath, r'Bayes')  # 贝叶斯文件路径
    entityAndRelationshipPath = os.path.join(fileRootPath, r"entityAndRelationship")  # 三元组文件路径
    logFilePath = os.path.join(fileRootPath, r"log")  # 日志文件路径
    otherConfigurationPath = os.path.join(configurationPath, r"other")  # 其他配置的路径
    urlTableMapPath = os.path.join(configurationPath, r'urlTableMap')  # url必提表格映射 路径
    personTablePath = os.path.join(configurationPath, r'PersonTable')  # 人物表格映射 路径
    jsonPath = os.path.join(fileRootPath, r"json")  # json文件所在路径
    SeedPath = os.path.join(spiderFilePath, 'PersonUrlSeedLink.txt')  # 种子文件路径

    gol.set_value('projectPath', projectPath)
    gol.set_value('fileRootPath', fileRootPath)
    gol.set_value('spiderFilePath', spiderFilePath)
    gol.set_value('tableDocPath', tableDocPath)
    gol.set_value('tablePklPath', tablePklPath)
    gol.set_value('imagesPath', imagesPath)
    gol.set_value('TriadPath', TriadPath)
    gol.set_value('configurationPath', configurationPath)
    gol.set_value('BayesFilePath', BayesFilePath)
    gol.set_value('entityAndRelationshipPath', entityAndRelationshipPath)
    gol.set_value('logFilePath', logFilePath)
    gol.set_value('otherConfigurationPath', otherConfigurationPath)
    gol.set_value('urlTableMapPath', urlTableMapPath)
    gol.set_value('personTablePath', personTablePath)
    gol.set_value('jsonPath', jsonPath)
    gol.set_value('SeedPath', SeedPath)

    pathDict = gol.getGlobalDictCopy()
    for pathName in pathDict.keys():
        if not os.path.exists(pathDict[pathName]):
            os.makedirs(pathDict[pathName])


def deleteAll():
    spider = WebSpider()
    spider.sql.clearAllTable()


def main():
    spider = WebSpider()
    tableExtractor = TableExtract()
    personGraph = PersonGraph()
    # tableExtractor.test()
    spider.start(threadsNum=2, maxCount=float('inf'))  # 爬虫执行无数次
    threading.Thread(target=spider.dealWithUselessUrl).start()
    threading.Thread(target=tableExtractor.start).start()
    threading.Thread(target=personGraph.start).start()


def test():
    tableExtractor = TableExtract()
    tableExtractor.test()
    # filenameList = [
    #     # r"E:\Programe\Code\python\pythonProject\WebTableExtractionSystem\file\tableDoc\新家谱登记表.docx",
    #     # r"E:\Programe\Code\python\pythonProject\WebTableExtractionSystem\file\tableDoc\zhailingwushi.docx",
    #     # r"E:\Programe\Code\python\pythonProject\WebTableExtractionSystem\file\tableDoc\旧版马吴登记表.docx",
    #     r"E:\Programe\Code\python\pythonProject\WebTableExtractionSystem\file\tableDoc\example.docx"
    # ]
    # for filename in filenameList:
    #     tableList = extractWordTable(filename)
    #     for table in tableList:
    #         table.writeTable2Doc(f"{gol.get_value('tableDocPath')}\\myTest.docx")


if __name__ == "__main__":
    initial()
    # deleteAll()
    # main()
    # test()
    # startWindows()
    bayes = Bayes()
    bayes.start()
