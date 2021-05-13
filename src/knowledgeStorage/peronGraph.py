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
    def __init__(self):
        self.g = Graph("http://localhost:7474", username="neo4j", password="h132271350570")  # 这里填自己的信息
        self.sql = SqlServerProcessor()
        self.running = False

    def createNodesFromCsv(self, filename="entity.json"):
        entityAndRelationshipPath = gol.get_value('entityAndRelationshipPath')
        with open(f"{entityAndRelationshipPath}\\{filename}", mode='r+', encoding='utf-8') as f:
            line = f.readline()  # 调用文件的 readline()方法
            while line:
                entityList = json.loads(line)
                self.__createNodeWithEntityList("person", entityList)
                line = f.readline()

    def createRelationshipsFromCsv(self, filename="relationship.csv"):

        matcher = NodeMatcher(self.g)
        with open(f"{gol.get_value('entityAndRelationshipPath')}\\{filename}", 'r', encoding="utf-8") as csvFile:
            reader = csv.reader(csvFile)
            for row in reader:
                subjectName = row[0]
                relationshipName = row[1]
                objectName = row[2]
                # 匹配节点
                node1 = matcher.match("person", name=subjectName).first()
                if node1 is None:
                    node1 = Node("person", name=subjectName)
                    self.g.create(node1)
                node1 = matcher.match("person", name=subjectName).first()
                node2 = matcher.match("person", name=objectName).first()
                if node2 is None:
                    node2 = Node("person", name=objectName)
                    self.g.create(node2)
                self.__createRelationship(node1, node2, relationshipName)

    @except_output()
    def __createNodeWithEntityList(self, label: str, entityList: list):
        """
        依靠实体列表创建节点
        :param label:
        :param entityList:
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
        for key in propertyDict:
            if key not in node:
                node[key] = propertyDict[key]
        self.g.merge(node)
        print(f">>>>已经融合<{node['name']}>")

    def __createRelationship(self, start_node: Node, rel_name: str, end_node: Node):
        try:
            rel = Relationship(start_node, rel_name, end_node)
            self.g.create(rel)
            print(f">>>>创建关系<{start_node['name']},{rel_name},{end_node['name']}>")
        except Exception as e:
            print(e)

    @except_output()
    def __creteRelationshipsWithList(self, triadList: list):
        for triad in triadList:
            node1 = self.__getNode("person", triad[0][0], triad[0][1])
            relationship = triad[1]
            node2 = self.__getNode("person", triad[2][0], triad[2][1])
            self.__createRelationship(node1, relationship, node2)

    def __getNode(self, label: str, name: str, url: str = ''):
        """
        按照一定条件查找是否存在该节点，不存在则创建
        :param url:
        :param label:
        :param name:
        :return:
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
        self.running = True
        print("开始构建知识图谱...")
        pendingQueue = Queue(maxsize=200)
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
                    self.__createNodeWithEntityList("person", entityList)
                if relationshipTriadList is not None and len(relationshipTriadList) != 0:
                    self.__creteRelationshipsWithList(relationshipTriadList)

                self.addQueue(pendingQueue, 'entityAndRelationship')

            time.sleep(0.2)
            waitTimes -= 1

    def stop(self):
        self.running = False

    def addQueue(self, QueueName: Queue, tableName: str):
        # 队列长度小于一半，则从数据库中补充到队列
        if QueueName.qsize() < int(QueueName.maxsize / 10):
            for url in self.sql.getERFromDB(tableName, int(QueueName.maxsize / 2)):
                QueueName.put(url)
            self.sql.deleteFromDBWithIdNum(tableName, int(QueueName.maxsize / 2))  # 删除
