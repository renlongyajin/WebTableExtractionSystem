#!/user/bin/env python3
# -*- coding: utf-8 -*-
import json
from py2neo import Graph, Node, NodeMatcher, Relationship
import csv

from src.app import gol


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
            nodelist = list(self.g.nodes.match())
            # -- create if not exists
            # 节点匹配对象
            matcher = NodeMatcher(self.g)
            # 匹配节点
            node = matcher.match(label, name=name).first()
            if node is None:
                node = Node(label, **entityDict, name=name)
                try:
                    self.g.create(node)
                    # self.g.push(node)
                except Exception as e:
                    print(e)
            else:
                print(f"当前节点已经存在<{name}>")

            pass

    def __createRelationship(self, start_node: Node, end_node: Node, rel_name: str):
        try:
            rel = Relationship(start_node, rel_name, end_node)
            self.g.create(rel)
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
