import json
import os
import sys
import threading
import time

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


class MainWindows(QMainWindow):
    def __init__(self):
        super(MainWindows, self).__init__()

        self.topRight = QWidget(self)
        self.topMid = Neo4jWindows()
        self.topLeft = QWidget(self)
        self.relationshipEditor = QPlainTextEdit()
        self.entityEditor = QPlainTextEdit()
        self.knowGraphThread = KnowGraphThread()
        self.neo4jWindows = Neo4jWindows()
        self.dealWithUselessUrlThread = DealWithUselessUrlThread()
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
        self.tableSplitter = QSplitter(Qt.Vertical)
        self.tableSplitter.addWidget(tableWidget)
        self.tableLayout.addWidget(self.tableSplitter)

        self.topRight.setLayout(self.tableLayout)
        self.topLeft.setLayout(self.listLayout)

        h_layout = QHBoxLayout()

        bottom = QWidget(self)

        topSplitter = QSplitter(Qt.Horizontal)
        topSplitter.addWidget(self.topLeft)
        topSplitter.addWidget(self.topMid)
        topSplitter.addWidget(self.topRight)
        topSplitter.setStretchFactor(0, 4)
        topSplitter.setStretchFactor(1, 4)
        topSplitter.setStretchFactor(2, 3)

        bottomSplitter = QSplitter(Qt.Horizontal)
        # bottomSplitter.addWidget(bottom)
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
        file.addAction(save_)
        file.addAction(add_)
        file.addAction(open_)
        # open_.triggered.connect(self.loadText)

        run_ = bar.addMenu("运行")
        runAll_ = QAction("所有所有", self)
        runSpider_ = QAction("运行爬虫", self)
        run_.addAction(runAll_)
        run_.addAction(runSpider_)
        runAll_.triggered.connect(self.runAll)
        runSpider_.triggered.connect(self.runSpider)

        database_ = bar.addMenu("数据库")
        sqlServer_ = QAction("sqlServer", self)
        neo4j_ = QAction("neo4j", self)
        database_.addAction(sqlServer_)
        database_.addAction(neo4j_)
        neo4j_.triggered.connect(self.neo4jWindows.show)

        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)

    def loadText(self):
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.AnyFile)
        dialog.setFilter(QDir.Files)

        if dialog.exec():
            filenames = dialog.selectedFiles()
            f = open(filenames[0], encoding='utf-8', mode='r')
            with f:
                data = f.read()
                self.contents.setText(data)

    def runSpider(self):
        if not self.webSpiderRunning:
            self.webSpiderThread.start()
            self.dealWithUselessUrlThread.start()
            self.webSpiderRunning = True

    def runTableExtractor(self):
        if not self.tableExtractorRunning:
            self.tableExtractorThread.SUpdateUrlList.connect(self.displayUrl)
            self.tableExtractorThread.start()
            self.tableExtractorRunning = True

    def runKnowGraph(self):
        if not self.knowGraphRunning:
            self.knowGraphThread.start()
            self.knowGraphRunning = True

    def runAll(self):
        self.runSpider()
        self.runTableExtractor()
        self.runKnowGraph()

    def displayUrl(self, urlList: list):
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
        tableSplitter = QSplitter(Qt.Vertical)
        for table in tableList:
            addTable(tableSplitter, table)
        self.tableLayout.addWidget(tableSplitter)

    def startNeo4jWindows(self):
        self.neo4jWindows.show()


class WebSpiderThread(QThread):
    def __init__(self):
        super().__init__()
        self.webSpider = WebSpider()

    def run(self):
        self.webSpider.start(2, float('inf'))


class DealWithUselessUrlThread(QThread):
    def __init__(self):
        super().__init__()
        self.webSpider = WebSpider()

    def run(self):
        self.webSpider.dealWithUselessUrl()


class TableExtractorThread(QThread):
    SUpdateUrlList = pyqtSignal(list)

    def __init__(self):
        super().__init__()
        self.tableExtractor = TableExtract()

    def run(self):
        threading.Thread(target=self.tableExtractor.start).start()
        while True:
            self.SUpdateUrlList.emit(self.tableExtractor.urlList)
            time.sleep(1.0)


class KnowGraphThread(QThread):
    def __init__(self):
        super().__init__()
        self.personGraph = PersonGraph()

    def run(self):
        self.personGraph.start()


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
