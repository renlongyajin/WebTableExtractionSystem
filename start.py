# -*- coding:utf-8 -*-

import os
from os.path import dirname, abspath
import threading
import src.app.gol as gol
from src.spider.WebSpider import WebSpider
from src.tableExtract.tableExtractor import TableExtract
from src.knowledgeStorage.peronGraph import PersonGraph


def initial():
    gol.init()  # 先必须在主模块初始化（只在Main模块需要一次即可）
    projectPath = os.path.abspath(os.path.dirname(__file__))  # 当前项目所在路径
    fileRootPath = os.path.join(projectPath, r"file")  # 文件根目录
    spiderFilePath = os.path.join(fileRootPath, r'spider')  # 爬虫的文件所在路径
    tableDocPath = os.path.join(fileRootPath, r'tableDoc')  # 表格Doc路径
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


if __name__ == "__main__":
    initial()
    # spider = WebSpider()
    # threading.Thread(target=spider.start, args=(float('inf'),)).start()  # 爬虫执行无数次
    tableExtractor = TableExtract()
    threading.Thread(target=tableExtractor.test).start()
    # personGraph = PersonGraph()
    # threading.Thread(target=personGraph.start).start()
