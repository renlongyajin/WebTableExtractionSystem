#!/user/bin/env python3
# -*- coding: utf-8 -*-

from PyQt5.QtCore import QUrl, Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLineEdit, QPushButton, QHBoxLayout, QLabel


class Neo4jWindows(QWidget):
    def __init__(self):
        super(Neo4jWindows, self).__init__()
        self.resize(1000, 600)

        self.back_btn = QPushButton(self)
        self.forward_btn = QPushButton(self)
        self.refresh_btn = QPushButton(self)
        self.zoom_in_btn = QPushButton(self)
        self.zoom_out_btn = QPushButton(self)
        self.zoom_text = QLabel(self)
        self.url_le = QLineEdit(self)

        self.browser = QWebEngineView()

        self.h_layout = QHBoxLayout()
        self.v_layout = QVBoxLayout()

        self.layout_init()
        self.btn_init()
        self.le_init()
        self.browser_init()

        self.proportion = 0.1

    def layout_init(self):
        self.h_layout.setSpacing(0)
        self.h_layout.addWidget(self.back_btn)
        self.h_layout.addWidget(self.forward_btn)
        self.h_layout.addWidget(self.refresh_btn)
        self.h_layout.addStretch(2)
        self.h_layout.addWidget(self.url_le)
        self.h_layout.addStretch(2)

        self.h_layout.addWidget(self.zoom_text)
        self.h_layout.addWidget(self.zoom_in_btn)
        self.h_layout.addWidget(self.zoom_out_btn)

        self.v_layout.addLayout(self.h_layout)
        self.v_layout.addWidget(self.browser)

        self.setLayout(self.v_layout)

    def browser_init(self):
        self.browser.load(QUrl('http://localhost:7474/browser/'))
        self.browser.urlChanged.connect(lambda: self.url_le.setText(self.browser.url().toDisplayString()))

    def btn_init(self):
        self.back_btn.setIcon(QIcon('images/back.png'))
        self.forward_btn.setIcon(QIcon('images/forward.png'))
        self.refresh_btn.setIcon(QIcon('images/refresh.png'))
        self.zoom_in_btn.setIcon(QIcon('images/zoom_in.png'))
        self.zoom_out_btn.setIcon(QIcon('images/zoom_out.png'))

        self.back_btn.setText("后退")
        self.forward_btn.setText("前进")
        self.refresh_btn.setText("刷新")
        self.zoom_in_btn.setText("放大")
        self.zoom_out_btn.setText("缩小")
        self.zoom_text.setText(f"缩放比例:{self.browser.zoomFactor()}")

        self.back_btn.clicked.connect(self.browser.back)
        self.forward_btn.clicked.connect(self.browser.forward)
        self.refresh_btn.clicked.connect(self.browser.reload)
        self.zoom_in_btn.clicked.connect(self.zoom_in_func)
        self.zoom_out_btn.clicked.connect(self.zoom_out_func)

    def le_init(self):
        self.url_le.setFixedWidth(400)
        self.url_le.setPlaceholderText('Search or enter website name')

    def keyPressEvent(self, QKeyEvent):
        if QKeyEvent.key() == Qt.Key_Return or QKeyEvent.key() == Qt.Key_Enter:
            if self.url_le.hasFocus():
                if self.url_le.text().startswith('https://') or self.url_le.text().startswith('http://'):
                    self.browser.load(QUrl(self.url_le.text()))
                else:
                    self.browser.load(QUrl('https://' + self.url_le.text()))

    def updatePageWithUrl(self, url: str):
        self.browser.load(QUrl(url))

    def zoom_in_func(self):
        self.browser.setZoomFactor(self.browser.zoomFactor() + self.proportion)
        self.zoom_text.setText(f"缩放比例:{round(self.browser.zoomFactor(), 1)}")

    def zoom_out_func(self):
        self.browser.setZoomFactor(self.browser.zoomFactor() - self.proportion)
        self.zoom_text.setText(f"缩放比例:{round(self.browser.zoomFactor(), 1)}")
