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
        # self.g.delete_all()  # 将之前的图  全部删除
        pass

    def createNodes(self):
        entityAndRelationshipPath = gol.get_value('entityAndRelationshipPath')
        with open(f"{entityAndRelationshipPath}\\entity.json", mode='r+', encoding='utf-8') as f:
            line = f.readline()  # 调用文件的 readline()方法
            while line:
                entityList = json.loads(line)
                self.__createNodeWithEntityList("person", entityList)
                line = f.readline()

    def __createNodeWithEntityList(self, label: str, entityList: list):
        """
        依靠实体列表创建节点
        :param label:
        :param entityList:
        :return:
        """
        for entity in entityList:
            name = entity[0]
            entityDict = entity[1]
            # -- create if not exists
            # 节点匹配对象
            matcher = NodeMatcher(self.g)
            # 匹配节点
            node = matcher.match(label, name=name).first()
            if node is None:
                node = Node(label, **entityDict, name=name)
                try:
                    self.g.create(node)
                    print(f"创建节点<{name}>")
                except Exception as e:
                    print(e)
            else:
                print(f"当前节点已经存在<{name}>")

    def __createRelationship(self, start_node: Node, rel_name: str, end_node: Node):
        try:
            rel = Relationship(start_node, rel_name, end_node)
            self.g.create(rel)
            print(f"创建关系<{start_node['name']},{rel_name},{end_node['name']}>")
        except Exception as e:
            print(e)

    def createRelationships(self):

        matcher = NodeMatcher(self.g)
        with open(f"{gol.get_value('entityAndRelationshipPath')}\\relationship.csv", 'r', encoding="utf-8") as csvFile:
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

    def creteRelationshipsWithList(self, triadList: list):
        for triad in triadList:
            node1 = self.getNode("person", triad[0])
            relationship = triad[1]
            node2 = self.getNode("person", triad[2])
            self.__createRelationship(node1, relationship, node2)

    def getNode(self, label: str, name: str):
        """
        按照一定条件查找是否存在该节点，不存在则创建
        :param label:
        :param name:
        :return:
        """
        matcher = NodeMatcher(self.g)
        node = matcher.match(label, name=name).first()
        if node:
            return node
            # if node['url'] is not None:
            #     if False:  # TODO:这里有一个判断条件，如果url相同则返回
            #         return node
        node = Node(label, name=name)
        try:
            self.g.create(node)  # TODO:如何返回自己刚刚创建的节点
            return node
        except Exception as e:
            print(e)

    @except_output()
    def start(self):
        pendingQueue = Queue(maxsize=200)
        maxWaitTimes = 100
        waitTimes = maxWaitTimes
        sql = SqlServerProcessor()
        while waitTimes:
            ERList = sql.readERFromDB('entityAndRelationship', int(pendingQueue.maxsize / 2))
            sql.deleteFromDBWithIdNum("entityAndRelationship", int(pendingQueue.maxsize / 2))
            for ER in ERList:
                pendingQueue.put_nowait(ER)

            while not pendingQueue.empty():
                waitTimes = maxWaitTimes
                ER = pendingQueue.get_nowait()
                entityList = json.loads(ER[0])
                relationshipTriadList = json.loads(ER[1])
                if entityList is not None and len(entityList) != 0:
                    self.__createNodeWithEntityList("person", entityList)
                if relationshipTriadList is not None and len(relationshipTriadList) != 0:
                    self.creteRelationshipsWithList(relationshipTriadList)

                if pendingQueue.qsize() < int(pendingQueue.maxsize / 2):
                    for data in sql.readERFromDB("entityAndRelationship", int(pendingQueue.maxsize / 2)):
                        pendingQueue.put(data)
                    sql.deleteFromDBWithIdNum("entityAndRelationship", int(pendingQueue.maxsize / 2))

            time.sleep(1.0)
            waitTimes -= 1
