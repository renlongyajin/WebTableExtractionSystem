#!/user/bin/env python3
# -*- coding: utf-8 -*-
import json
import time
from queue import Queue

from py2neo import Graph, Node, NodeMatcher, Relationship
import csv

from src.IO.databaseInteraction.MSSQL import SqlServerProcessor
from src.app import gol
from src.tools.algorithm.exceptionCatch import except_output


class PersonGraph:
    def __init__(self, host: str = "http://localhost:7474",
                 username: str = "neo4j",
                 password: str = "h132271350570"):
        """
        初始化人物图谱，链接到neo4j图数据库
        :param host: 连接地址
        :param username: 用户名
        :param password: 密码
        """
        self.host = host
        self.username = username
        self.password = password
        self.g = Graph(self.host, username=self.username, password=self.password)
        self.sql = SqlServerProcessor()
        self.running = False

    @except_output()
    def __createNodeWithEntityList(self, label: str, entityList: list):
        """
        依靠标签和实体列表创建节点
        :param label:标签
        :param entityList:实体列表
        :return:
        """
        for entity in entityList:
            name = entity[0][0]
            url = entity[0][1]
            entityDict_ = entity[1]
            # -- create if not exists
            # 节点匹配对象
            matcher = NodeMatcher(self.g)
            # 匹配节点
            try:
                if name.endswith("氏"):
                    node = matcher.match(label, name=name).first()
                    self.g.create(node)
                    print(f">>>>创建节点<{name}>")
                elif len(url) == 0 or url.isspace():
                    node = matcher.match(label, name=name).first()
                    if node is None:
                        node = Node(label, **entityDict_, name=name)
                        self.g.create(node)
                        print(f">>>>创建节点<{name}>")
                    else:
                        self.fusionNode(node, entityDict_)
                else:
                    node = matcher.match(label, name=name, url=url).first()
                    if node is None:
                        node = matcher.match(label, name=name).first()
                        if node:
                            if node['url'] in url or url in node['url']:  # 如果url相互包含，则一定是同一个
                                self.fusionNode(node, entityDict_)
                                return
                        node = Node(label, **entityDict_, name=name, url=url)
                        self.g.create(node)
                        print(f">>>>创建节点<{name}>")
                    else:
                        self.fusionNode(node, entityDict_)
            except Exception as e:
                print(e)

    def fusionNode(self, node: Node, propertyDict: dict):
        """
        将节点与属性字典融合
        :param node: 待融合的节点
        :param propertyDict: 待融合的属性字典
        :return: 无
        """
        for key in propertyDict:
            if key not in node:
                node[key] = propertyDict[key]
        self.g.merge(node)
        print(f">>>>已经融合<{node['name']}>")

    def __createRelationship(self, start_node: Node, rel_name: str, end_node: Node):
        """
        创建一条关系
        :param start_node:起始节点
        :param rel_name: 关系名
        :param end_node: 结束节点
        :return: 无
        """
        try:
            rel = Relationship(start_node, rel_name, end_node)
            self.g.create(rel)
            print(f">>>>创建关系<{start_node['name']},{rel_name},{end_node['name']}>")
        except Exception as e:
            print(e)

    @except_output()
    def __creteRelationshipsWithList(self, triadList: list):
        """
        用三元组列表创建多条关系
        :param triadList: 三元组列表，例如[['小王'，'爸爸'，'老王'],['小白','哥哥','大白']]
        :return: 无
        """
        for triad in triadList:
            node1 = self.__getNode("person", triad[0][0], triad[0][1])
            relationship = triad[1]
            node2 = self.__getNode("person", triad[2][0], triad[2][1])
            self.__createRelationship(node1, relationship, node2)

    def __getNode(self, label: str, name: str, url: str = ''):
        """
        按照一定条件查找是否存在该节点，不存在则创建
        :param url:查找该url链接的节点
        :param label:查找该标签的节点
        :param name:查找该名称的节点
        :return:返回节点
        """
        matcher = NodeMatcher(self.g)
        if len(url) != 0 and not url.isspace():
            node = matcher.match(label, name=name, url=url).first()
            if node:
                return node
        if name.endswith("氏"):
            node = Node(label, name=name)
        else:
            node = matcher.match(label, name=name).first()
            if node:
                return node
            else:
                node = Node(label, name=name)
        return node

    @except_output()
    def start(self, maxWaitTimes=float('inf')):
        """
        开始构建知识图谱
        :param maxWaitTimes:
        :return:
        """
        self.running = True
        print("开始构建知识图谱...")
        pendingQueue = Queue(maxsize=200)  # 数据队列
        waitTimes = maxWaitTimes
        while waitTimes:
            if not self.running:
                return
            self.addQueue(pendingQueue, 'entityAndRelationship')
            while not pendingQueue.empty():
                if not self.running:
                    return
                waitTimes = maxWaitTimes
                ER = pendingQueue.get_nowait()
                entityList = json.loads(ER[0])
                relationshipTriadList = json.loads(ER[1])
                if entityList is not None and len(entityList) != 0:
                    self.__createNodeWithEntityList("person", entityList)  # 创建节点
                if relationshipTriadList is not None and len(relationshipTriadList) != 0:
                    self.__creteRelationshipsWithList(relationshipTriadList)  # 创建关系

                self.addQueue(pendingQueue, 'entityAndRelationship')

            time.sleep(0.2)
            waitTimes -= 1

    def stop(self):
        """
        停止构建知识图谱
        :return:
        """
        self.running = False

    def addQueue(self, QueueName: Queue, tableName: str):
        """
        从数据库表中读取数据，补充到队列
        :param QueueName:待补充的队列名称
        :param tableName: 数据表名
        :return: 无
        """
        # 队列长度小于最大长度的1/10，则从数据库中补充到队列
        if QueueName.qsize() < int(QueueName.maxsize / 10):
            for url in self.sql.getERFromDB(tableName, int(QueueName.maxsize / 2)):
                QueueName.put(url)
            self.sql.deleteFromDBWithIdNum(tableName, int(QueueName.maxsize / 2))  # 删除
