#!/user/bin/env python3
# -*- coding: utf-8 -*-
import sys

from PyQt5.QtWidgets import QTabWidget, QWidget, QFormLayout, QLineEdit, QHBoxLayout, QLabel, QApplication, QPushButton, \
    QVBoxLayout, QDesktopWidget

from src.IO.fileInteraction.FileIO import FileIO
from src.app import gol


class ConfigureWindows(QWidget):
    def __init__(self, parent=None):
        super(ConfigureWindows, self).__init__(parent)
        self.setWindowTitle("设置")
        layout = QVBoxLayout()
        configureWindow = ConfigureWindow()
        layout.addWidget(configureWindow)
        self.setLayout(layout)


class ConfigureWindow(QTabWidget):
    def __init__(self, parent=None):
        super(ConfigureWindow, self).__init__(parent)
        # 创建用于显示控件的窗口
        self.tab1 = QWidget()
        self.tab2 = QWidget()
        self.tab3 = QWidget()

        self.addTab(self.tab1, '爬虫配置')
        self.addTab(self.tab2, '抽取系统配置')
        self.addTab(self.tab3, '图谱配置')

        self.tab1UI()
        self.tab2UI()
        self.tab3UI()
        self.type = ''
        self.resize(600, 600)
        self.center()

    def tab1UI(self):
        filepath = f"{gol.get_value('configurationPath')}\\spider.json"
        self.RenderPage(self.tab1, filepath)

    def tab2UI(self):
        filepath = f"{gol.get_value('configurationPath')}\\tableExtractor.json"
        self.RenderPage(self.tab2, filepath)

    def tab3UI(self):
        filepath = f"{gol.get_value('configurationPath')}\\knowGraph.json"
        self.RenderPage(self.tab3, filepath)

    def RenderPage(self, tab, filepath: str):
        configureDict = FileIO.readJson(filepath)
        if configureDict:
            self.configurePage(tab, configureDict)

    @staticmethod
    def configurePage(tab, configureDict: dict):
        v_layout = QVBoxLayout()
        h_layout = QHBoxLayout()
        formLayout = QFormLayout()
        for key in configureDict:
            Label = QLabel(str(key))
            Edit = QLineEdit()
            Edit.setText(str(configureDict[key]))
            formLayout.addRow(Label, Edit)

        confirmButton = QPushButton("确认")
        cancelButton = QPushButton("取消")

        h_layout.addWidget(confirmButton)
        h_layout.addWidget(cancelButton)
        v_layout.addLayout(formLayout)
        v_layout.addLayout(h_layout)
        tab.setLayout(v_layout)

    def center(self):
        # 获取屏幕坐标系
        screen = QDesktopWidget().screenGeometry()
        # 获取窗口坐标系
        size = self.geometry()
        newLeft = (screen.width() - size.width()) / 2
        newTop = (screen.height() - size.height()) / 2
        self.move(newLeft, newTop)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    demo = ConfigureWindows()
    demo.show()
    sys.exit(app.exec_())
