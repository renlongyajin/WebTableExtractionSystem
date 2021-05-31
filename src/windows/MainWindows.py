import json
import os
import sys
import threading
import time
from functools import partial

from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import *

from src.IO.fileInteraction.FileIO import FileIO
from src.app import gol
# noinspection PyArgumentList
from src.knowledgeStorage.peronGraph import PersonGraph
from src.spider.WebSpider import WebSpider
from src.tableExtract.table import Table
from src.tableExtract.tableExtractor import TableExtract
from src.windows.Neo4jWindows import Neo4jWindows
from src.windows.configureWindows import ConfigureWindows
from src.windows.notepad import Notepad


class MainWindows(QMainWindow):
    def __init__(self):
        super(MainWindows, self).__init__()

        self.personName_text = QLabel(self)
        self.personName_text.setText("人名：")
        self.search_btn = QPushButton(self)
        self.search_btn.setText("加入")

        self.url_le = QLineEdit(self)
        self.neo4jWindows = Neo4jWindows()

        self.statusBar = QStatusBar()
        self.tableSplitter = QSplitter(Qt.Vertical)

        self.topRight = QWidget(self)
        self.topMid = Neo4jWindows()
        self.topLeft = QWidget(self)

        self.relationshipEditor = QPlainTextEdit()
        self.entityEditor = QPlainTextEdit()

        self.knowGraphThread = KnowGraphThread()
        self.webSpiderThread = WebSpiderThread()
        self.tableExtractorThread = TableExtractorThread()

        self.browserLayout = QVBoxLayout()
        self.listLayout = QVBoxLayout()
        self.tableLayout = QVBoxLayout()

        self.centerWidget = QWidget()
        self.listView = QListView()
        self.listModel = QStringListModel()
        self.browser = QWebEngineView()

        self.webSpider = WebSpider()
        self.tableExtractor = TableExtract()
        self.urlList = []
        self.webSpiderRunning = False
        self.tableExtractorRunning = False
        self.knowGraphRunning = False

        self.initUI()

        threading.Thread(target=self.tableExtractorThread.updateUrlList).start()

    # noinspection PyArgumentList
    def initUI(self):
        """
        初始化页面UI
        :return:
        """
        self.setWindowTitle('Web表格信息抽取系统')
        self.setMenu()

        hBox = QHBoxLayout()

        self.listModel.setStringList(self.urlList)
        self.listView.setModel(self.listModel)
        self.listView.clicked.connect(self.showUrlPage)
        self.listView.clicked.connect(self.showExtractedInformation)
        self.search_btn.clicked.connect(self.btn_click_search)
        self.listLayout.addWidget(self.listView)
        hAddUrlBox = QHBoxLayout()
        hAddUrlBox.addWidget(self.personName_text)
        hAddUrlBox.addWidget(self.url_le)
        hAddUrlBox.addWidget(self.search_btn)
        self.listLayout.addLayout(hAddUrlBox)
        self.browser.load(QUrl('https://www.baidu.com/'))
        self.browserLayout.addWidget(self.browser)

        tableWidget = QTableWidget()
        self.tableSplitter.addWidget(tableWidget)
        self.tableLayout.addWidget(self.tableSplitter)

        self.topRight.setLayout(self.tableLayout)
        self.topLeft.setLayout(self.listLayout)

        topSplitter = QSplitter(Qt.Horizontal)
        topSplitter.addWidget(self.topLeft)
        topSplitter.addWidget(self.topMid)
        topSplitter.addWidget(self.topRight)
        topSplitter.setStretchFactor(0, 4)
        topSplitter.setStretchFactor(1, 4)
        topSplitter.setStretchFactor(2, 3)

        bottomSplitter = QSplitter(Qt.Horizontal)
        bottomSplitter.addWidget(self.entityEditor)
        bottomSplitter.addWidget(self.relationshipEditor)

        allWindows = QSplitter(Qt.Vertical)
        allWindows.addWidget(topSplitter)
        allWindows.addWidget(bottomSplitter)
        allWindows.setSizes([300, 100])

        hBox.addWidget(allWindows)
        self.centerWidget.setLayout(hBox)
        self.setCentralWidget(self.centerWidget)

    def setMenu(self):
        """
        设置菜单栏
        :return:
        """
        bar = self.menuBar()  # 获取菜单栏
        file = bar.addMenu("文件")
        add_ = QAction("新建", self)
        add_.setShortcut("Ctrl + N")
        save_ = QAction("保存", self)
        save_.setShortcut("Ctrl + S")
        open_ = QAction("打开", self)
        open_.setShortcut("Ctrl + O")
        openSeed_ = QAction("打开种子文件", self)
        file.addAction(save_)
        file.addAction(add_)
        file.addAction(open_)
        file.addAction(openSeed_)
        open_.triggered.connect(self.loadText)
        openSeed_.triggered.connect(self.openSeed)

        run_ = bar.addMenu("运行")
        runAll_ = QAction("所有所有", self)
        runSpider_ = QAction("运行爬虫", self)
        runTableExtractor_ = QAction("运行抽取程序", self)
        runKnowGraph_ = QAction("运行图谱构建程序", self)
        runAll_.triggered.connect(self.runAll)
        runSpider_.triggered.connect(self.runSpider)
        runTableExtractor_.triggered.connect(self.runTableExtractor)
        runKnowGraph_.triggered.connect(self.runKnowGraph)

        stopAll_ = QAction("关闭所有", self)
        stopSpider_ = QAction("关闭爬虫", self)
        stopTableExtractor_ = QAction("关闭抽取程序", self)
        stopKnowGraph_ = QAction("关闭图谱构建程序", self)
        stopAll_.triggered.connect(self.stopAll)
        stopSpider_.triggered.connect(self.stopSpider)
        stopTableExtractor_.triggered.connect(self.stopTableExtractor)
        stopKnowGraph_.triggered.connect(self.stopKnowGraph)

        run_.addAction(runAll_)
        run_.addAction(runSpider_)
        run_.addAction(runTableExtractor_)
        run_.addAction(runKnowGraph_)
        run_.addAction(stopAll_)
        run_.addAction(stopSpider_)
        run_.addAction(stopTableExtractor_)
        run_.addAction(stopKnowGraph_)

        self.webSpiderThread.SLockSignal.connect(partial(runSpider_.setEnabled, False))
        self.webSpiderThread.SUnLockSignal.connect(partial(runSpider_.setEnabled, True))
        self.tableExtractorThread.SLockSignal.connect(partial(runTableExtractor_.setEnabled, False))
        self.tableExtractorThread.SUnLockSignal.connect(partial(runTableExtractor_.setEnabled, True))
        self.knowGraphThread.SLockSignal.connect(partial(runKnowGraph_.setEnabled, False))
        self.knowGraphThread.SUnLockSignal.connect(partial(runKnowGraph_.setEnabled, True))

        database_ = bar.addMenu("数据库")
        sqlServer_ = QAction("sqlServer", self)
        neo4j_ = QAction("neo4j", self)
        database_.addAction(sqlServer_)
        database_.addAction(neo4j_)
        neo4j_.triggered.connect(self.startNeo4jWindows)

        configure_ = bar.addMenu("设置")
        CAll_ = QAction("全部设置", self)
        CSpider_ = QAction("爬虫设置", self)
        CTableExtractor_ = QAction("抽取系统设置", self)
        CKnowGraph_ = QAction("图谱设置", self)

        CAll_.triggered.connect(self.configurePage)

        configure_.addAction(CAll_)
        configure_.addAction(CSpider_)
        configure_.addAction(CTableExtractor_)
        configure_.addAction(CKnowGraph_)
        self.setStatusBar(self.statusBar)

    def loadText(self):
        """
        载入txt到显示页面
        :return:
        """
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.AnyFile)
        dialog.setFilter(QDir.Files)

        if dialog.exec():
            filenames = dialog.selectedFiles()
            notepad = Notepad(self)
            notepad.showPageWithPath(filenames[0])
            notepad.show()

    def openSeed(self):
        """
        打开种子文件链接
        :return:
        """
        seedPath = self.webSpiderThread.webSpider.SeedPath
        notepad = Notepad(self)
        notepad.showPageWithPath(seedPath)
        notepad.show()

    def stopSpider(self):
        """
        停止爬虫程序
        :return:
        """
        if self.webSpiderThread.webSpider.running:
            self.webSpiderThread.webSpider.stop()

    def stopTableExtractor(self):
        """
        停止表格信息抽取程序
        :return:
        """
        if self.tableExtractorThread.tableExtractor.running:
            self.tableExtractorThread.tableExtractor.stop()

    def stopKnowGraph(self):
        """
        停止知识图谱构建程序
        :return:
        """
        if self.knowGraphThread.personGraph.running:
            self.knowGraphThread.personGraph.stop()

    def stopAll(self):
        """
        停止所有程序
        :return:
        """
        self.stopSpider()
        self.stopTableExtractor()
        self.stopKnowGraph()

    def runSpider(self):
        """
        运行爬虫程序
        :return:
        """
        if not self.webSpiderThread.webSpider.running:
            self.webSpiderThread.start()

    def runTableExtractor(self):
        """
        运行表格信息抽取程序
        :return:
        """
        if not self.tableExtractorThread.tableExtractor.running:
            self.tableExtractorThread.SUpdateUrlList.connect(self.displayUrlList)
            self.tableExtractorThread.start()

    def runKnowGraph(self):
        """
        运行知识图谱构建程序
        :return:
        """
        if not self.knowGraphThread.personGraph.running:
            self.knowGraphThread.start()

    def runAll(self):
        """
        运行所有程序，包括爬虫、表格抽取器、知识图谱
        :return:
        """
        self.runSpider()
        self.runTableExtractor()
        self.runKnowGraph()

    def displayUrlList(self, urlList: list):
        """
        展示url列表
        :param urlList: url列表
        :return: 无
        """
        self.urlList = urlList
        self.listModel.setStringList(self.urlList)
        self.listView.setModel(self.listModel)

    def showUrlPage(self, item):
        """
        显示url所在的Web页面
        :param item: url所在的列表项，需要点击才能获得
        :return:
        """
        url = self.urlList[item.row()]
        # self.browser.load(QUrl(url))
        self.showWebPage(url)

    def showWebPage(self, url):
        """
        展示Web界面
        :param url:
        :return:
        """
        self.topMid.updatePageWithUrl(url)

    def showExtractedInformation(self, item):
        """
        展示已经抽取的信息
        :param item:
        :return:
        """
        url_ = self.urlList[item.row()]
        self.showTableAndChart(url_)

    def showTableAndChart(self, url_: str):
        """
        展示url对应的表格和关系图
        :param url_:输入的url
        :return: 无
        """

        def getDataListWithUrl(url: str):
            """
            根据url获取对应的数据：表格、关系图
            :param url: 指定的url
            :return:该rul对应的数据：表格、关系图
            """
            url2pathDict = FileIO.readPkl(f"{gol.get_value('configurationPath')}\\url2pathDict.pkl")
            wait = 10
            while url not in url2pathDict and wait > 0:
                time.sleep(0.5)
                url2pathDict = FileIO.readPkl(f"{gol.get_value('configurationPath')}\\url2pathDict.pkl")
                wait -= 1
            if wait <= 0:
                dataList_ = []
                relationChartPath = ''
            else:
                Id = url2pathDict[url]
                dataList_ = FileIO.readPkl(f"{gol.get_value('tablePklPath')}\\{Id}.pkl")
                relationChartPath = f"{gol.get_value('relationChartPath')}\\{Id}.html"
            return dataList_, relationChartPath

        def addTable(tabWidget: QTabWidget, table: Table):
            """
            向选项卡控件中添加表格
            :param tabWidget: 选项卡控件
            :param table: 表格
            :return:
            """
            tabWidget.setTabPosition(QTabWidget.East)
            tabWidget.setTabShape(QTabWidget.Triangular)

            tableWidget = QTableWidget()
            tableWidget.setRowCount(table.rowNumber)
            tableWidget.setColumnCount(table.colNumber)

            for rows in table.cell:
                for data in rows:
                    newItem = QTableWidgetItem(str(data.content))
                    tableWidget.setItem(data.absoluteRow, data.absoluteCol, newItem)
                    tableWidget.setSpan(data.absoluteRow, data.absoluteCol, data.rowspan, data.colspan)

            tabWidget.addTab(tableWidget, f"{table.name}")

        dataList, htmlPath = getDataListWithUrl(url_)
        if len(dataList) > 0:
            tableList = dataList[0]
            entityAndRelationshipList = dataList[1]
            # 获取实体列表
            entityList = [data[0] for data in entityAndRelationshipList]
            # 获取关系列表
            relationshipList = [data[1] for data in entityAndRelationshipList]
            # 实体转化为json串
            entityJson = json.dumps(entityList, sort_keys=True, indent=4, separators=(',', ':'), ensure_ascii=False)
            # 噶虚拟转化为json串
            relationshipJson = json.dumps(relationshipList, sort_keys=True, indent=4, separators=(',', ':'),
                                          ensure_ascii=False)
            # 可视化界面中，实体显示区，设置文本
            self.entityEditor.setPlainText(entityJson)
            # 可视化界面中，关系显示区，设置文本
            self.relationshipEditor.setPlainText(relationshipJson)
            # 将表格布局清空，便于放置新的表格
            clearLayOut(self.tableLayout)
            _tableSplitter = QSplitter(Qt.Vertical)
            tabWidget = QTabWidget()
            for _table in tableList:
                addTable(tabWidget, _table)
            _tableSplitter.addWidget(tabWidget)
            self.tableLayout.addWidget(_tableSplitter)
            # 显示关系图
            self.showRelationChart(htmlPath)
        else:
            QMessageBox.warning(self, "标题", f"未找到该人物", QMessageBox.Yes)

    def startNeo4jWindows(self):
        """
        开启neo4j窗口
        :return:
        """
        self.neo4jWindow = Neo4jWindows()
        self.neo4jWindow.show()

    def configurePage(self):
        """
        设置页面
        :return:
        """
        self.configureWindows = ConfigureWindows()
        self.configureWindows.show()

    def showRelationChart(self, htmlPath: str):
        """
        展示关系图
        :param htmlPath:
        :return:
        """
        self.relationChart = Neo4jWindows()
        self.relationChart.show()
        self.relationChart.showRelationChart(htmlPath)
        pass

    def btn_click_search(self):
        """
        输入人名后，点击"加入"按钮，触发该函数
        :return:
        """
        personName = self.url_le.text().strip()  # 人名去除空格
        if personName:
            url = "https://baike.baidu.com/item/" + personName.strip()  # 生成百科链接
            personName = personName.split('/')[0]  # 获取人名
            html = WebSpider.getHtml(url)  # 获取html串
            if html:
                tableExtractor = TableExtract()  # 构造表格抽取器
                tableExtractor.nowUrl = url  # 设置当前url
                tableExtractor.nowName = personName  # 设置当前人物名
                tableList = tableExtractor.getTable(html)  # 抽取表格
                tableExtractor.dealWithTableList(tableList)  # 处理表格，抽取信息，并且写入到文件和数据库
                self.tableExtractorThread.tableExtractor.addUrlList(url)  # 添加url到显示的列表中
                self.showWebPage(url)  # 展示该url对应的Web页面
                self.showTableAndChart(url)  # 展示该url中抽取的表格和关系图


class WebSpiderThread(QThread):
    SLockSignal = pyqtSignal()  # 锁信号量，点击后直到爬虫停止，无法点击第二次
    SUnLockSignal = pyqtSignal()  # 解锁信号，程序停止后，自动解开锁

    def __init__(self):
        super().__init__()
        self.webSpider = WebSpider()  # 爬虫程序

    def run(self):
        self.SLockSignal.emit()
        self.webSpider.start(2, float('inf'))  # 2个线程的爬虫，爬取次数为无限次
        while self.webSpider.running:
            time.sleep(0.5)
        self.SUnLockSignal.emit()


class TableExtractorThread(QThread):
    """
    表格抽取器的线程
    """
    SUpdateUrlList = pyqtSignal(list)  # 更新url列表的信号量
    SLockSignal = pyqtSignal()  # 锁信号量，只能点击一次按钮
    SUnLockSignal = pyqtSignal()  # 解锁信号量

    def __init__(self):
        super().__init__()
        self.tableExtractor = TableExtract()  # 表格信息抽取器

    def run(self):
        self.SLockSignal.emit()
        threading.Thread(target=self.tableExtractor.start).start()
        while self.tableExtractor.running:
            time.sleep(0.5)
        self.SUnLockSignal.emit()

    def updateUrlList(self):
        while True:
            self.SUpdateUrlList.emit(self.tableExtractor.urlList)
            time.sleep(1.0)

    def stopUpdateUrl(self):
        self.updateUrlRunning = False
        self.tableExtractor.stop()


class KnowGraphThread(QThread):
    """
    构建知识图谱的线程
    """
    SLockSignal = pyqtSignal()  # 锁信号
    SUnLockSignal = pyqtSignal()  # 解锁型号

    def __init__(self):
        super().__init__()
        self.personGraph = PersonGraph()  # 图谱构建程序

    def run(self):
        self.SLockSignal.emit()
        self.personGraph.start()
        self.SUnLockSignal.emit()


def clearLayOut(layOut: QLayout):
    """
    清空布局中所有控件
    :param layOut: 待清空的布局
    :return: 无
    """
    for i in range(layOut.count()):
        layOut.itemAt(i).widget().deleteLater()


def startWindows():
    """
    开始运行窗口
    :return: 无
    """
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(os.path.join(gol.get_value('imagesPath'), 'Dragon.ico')))
    mainWindows = MainWindows()
    mainWindows.showMaximized()

    sys.exit(app.exec_())


if __name__ == '__main__':
    startWindows()
