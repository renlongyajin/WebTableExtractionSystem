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

    # noinspection PyArgumentList
    def initUI(self):
        self.setWindowTitle('Web表格信息抽取系统')
        self.setMenu()

        hBox = QHBoxLayout()

        self.listModel.setStringList(self.urlList)
        self.listView.setModel(self.listModel)
        self.listView.clicked.connect(self.showUrlPage)
        self.listView.clicked.connect(self.showExtractedInformation)
        self.listLayout.addWidget(self.listView)

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
        neo4j_.triggered.connect(self.neo4jWindows.show)

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
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.AnyFile)
        dialog.setFilter(QDir.Files)

        if dialog.exec():
            filenames = dialog.selectedFiles()
            notepad = Notepad(self)
            notepad.showPageWithPath(filenames[0])
            notepad.show()

    def openSeed(self):
        seedPath = self.webSpiderThread.webSpider.SeedPath
        notepad = Notepad(self)
        notepad.showPageWithPath(seedPath)
        notepad.show()

    def stopSpider(self):
        if self.webSpiderThread.webSpider.running:
            self.webSpiderThread.webSpider.stop()

    def stopTableExtractor(self):
        if self.tableExtractorThread.tableExtractor.running:
            self.tableExtractorThread.tableExtractor.stop()

    def stopKnowGraph(self):
        if self.knowGraphThread.personGraph.running:
            self.knowGraphThread.personGraph.stop()

    def stopAll(self):
        self.stopSpider()
        self.stopTableExtractor()
        self.stopKnowGraph()

    def runSpider(self):
        if not self.webSpiderThread.webSpider.running:
            self.webSpiderThread.start()

    def runTableExtractor(self):
        if not self.tableExtractorThread.tableExtractor.running:
            self.tableExtractorThread.SUpdateUrlList.connect(self.displayUrlList)
            self.tableExtractorThread.start()

    def runKnowGraph(self):
        if not self.knowGraphThread.personGraph.running:
            self.knowGraphThread.start()

    def runAll(self):
        self.runSpider()
        self.runTableExtractor()
        self.runKnowGraph()

    def displayUrlList(self, urlList: list):
        self.urlList = urlList
        self.listModel.setStringList(self.urlList)
        self.listView.setModel(self.listModel)

    def showUrlPage(self, item):
        url = self.urlList[item.row()]
        # self.browser.load(QUrl(url))
        self.topMid.updatePageWithUrl(url)

    def showExtractedInformation(self, item):
        def getDataListWithUrl(url: str):
            url2pathDict = self.tableExtractorThread.tableExtractor.url2pathDict
            if url in url2pathDict:
                return FileIO.readPkl(url2pathDict[url])
            return []

        def addTable(tableSplitter: QSplitter, table: Table):
            tableWidget = QTableWidget()
            tableWidget.setRowCount(table.rowNumber)
            tableWidget.setColumnCount(table.colNumber)

            for rows in table.cell:
                for data in rows:
                    newItem = QTableWidgetItem(str(data.content))
                    tableWidget.setItem(data.absoluteRow, data.absoluteCol, newItem)
                    tableWidget.setSpan(data.absoluteRow, data.absoluteCol, data.rowspan, data.colspan)
            tableSplitter.addWidget(tableWidget)

        url_ = self.urlList[item.row()]
        dataList = getDataListWithUrl(url_)
        tableList = dataList[0]
        entityAndRelationshipList = dataList[1]
        entityList = [data[0] for data in entityAndRelationshipList]
        relationshipList = [data[1] for data in entityAndRelationshipList]
        entityJson = json.dumps(entityList, sort_keys=True, indent=4, separators=(',', ':'), ensure_ascii=False)
        relationshipJson = json.dumps(relationshipList, sort_keys=True, indent=4, separators=(',', ':'),
                                      ensure_ascii=False)
        self.entityEditor.setPlainText(entityJson)
        self.relationshipEditor.setPlainText(relationshipJson)
        clearLayOut(self.tableLayout)
        _tableSplitter = QSplitter(Qt.Vertical)
        for _table in tableList:
            addTable(_tableSplitter, _table)
        self.tableLayout.addWidget(_tableSplitter)

    def startNeo4jWindows(self):
        self.neo4jWindows.show()

    def configurePage(self):
        self.configureWindows = ConfigureWindows()
        self.configureWindows.show()


class WebSpiderThread(QThread):
    SLockSignal = pyqtSignal()
    SUnLockSignal = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.webSpider = WebSpider()

    def run(self):
        self.SLockSignal.emit()
        self.webSpider.start(2, float('inf'))
        while self.webSpider.running:
            time.sleep(0.5)
        self.SUnLockSignal.emit()


class TableExtractorThread(QThread):
    SUpdateUrlList = pyqtSignal(list)
    SLockSignal = pyqtSignal()
    SUnLockSignal = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.tableExtractor = TableExtract()
        self.updateUrlRunning = False

    def run(self):
        self.updateUrlRunning = True
        self.SLockSignal.emit()
        threading.Thread(target=self.tableExtractor.start).start()
        threading.Thread(target=self.updateUrlList).start()
        while self.tableExtractor.running:
            time.sleep(0.5)
        self.SUnLockSignal.emit()

    def updateUrlList(self):
        while True:
            if not self.updateUrlRunning:
                return
            self.SUpdateUrlList.emit(self.tableExtractor.urlList)
            time.sleep(1.0)

    def stopUpdateUrl(self):
        self.updateUrlRunning = False
        self.tableExtractor.stop()


class KnowGraphThread(QThread):
    SLockSignal = pyqtSignal()
    SUnLockSignal = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.personGraph = PersonGraph()

    def run(self):
        self.SLockSignal.emit()
        self.personGraph.start()
        self.SUnLockSignal.emit()


def clearLayOut(layOut: QLayout):
    for i in range(layOut.count()):
        layOut.itemAt(i).widget().deleteLater()


def startWindows():
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(os.path.join(gol.get_value('imagesPath'), 'Dragon.ico')))
    mainWindows = MainWindows()
    mainWindows.showMaximized()

    sys.exit(app.exec_())


if __name__ == '__main__':
    startWindows()
