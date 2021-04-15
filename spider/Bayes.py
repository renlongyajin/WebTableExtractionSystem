import codecs
import os
import re

import jieba
import joblib
import pandas
import pandas as pd
from numpy.ma import array
from sklearn import naive_bayes as bayes
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split

from IO.fileInteraction.FileIO import FileIO
from app import gol
from spider.WebSpider import WebSpider
from tableExtract.tableExtractor import TableExtract


class Bayes:
    def __init__(self):
        self.spiderFilePath = gol.get_value("spiderFilePath")
        self.urlRecordPath = os.path.join(self.spiderFilePath, 'urlRecord.pkl')  # url记录的文件路径
        self.uselessUrlRecordPath = os.path.join(self.spiderFilePath, 'uselessUrlRecord.pkl')  # 无用的url记录文件路径
        self.BayesFilePath = gol.get_value("BayesFilePath")
        self.urlListPath = os.path.join(self.BayesFilePath, 'urlList.pkl')
        self.uselessUrlListPath = os.path.join(self.BayesFilePath, 'uselessUrlList.pkl')
        self.stopwordsPath = os.path.join(self.BayesFilePath, 'stopwords.txt')
        self.trainDataPath = os.path.join(self.BayesFilePath, 'trainData.csv')
        self.charactersVocabularyPath = os.path.join(self.BayesFilePath, 'charactersVocabulary.m')
        self.bayesModelPath = os.path.join(self.BayesFilePath, 'bayesModel.m')
        self.stopwords = set(codecs.open(self.stopwordsPath, 'r', 'UTF8').read().split('\r\n'))

    def start(self):
        # urlList, uselessUrlList = self.getData()
        # self.transformData(urlList, uselessUrlList)
        # self.train()
        self.test()

    def getData(self, maxSize=100) -> (list, list):
        urlRecord = FileIO.readPkl(self.urlRecordPath)
        uselessUrlRecord = FileIO.readPkl(self.uselessUrlRecordPath)
        print(len(urlRecord))
        print(len(uselessUrlRecord))
        urlList = []
        uselessUrlList = []
        count = 0
        for url in urlRecord:
            urlList.append(url)
            count += 1
            if count >= maxSize:
                break

        count = 0
        for url in uselessUrlRecord:
            uselessUrlList.append(url)
            count += 1
            if count >= maxSize:
                break

        FileIO.writePkl(self.urlListPath, urlList)
        FileIO.writePkl(self.uselessUrlRecordPath, uselessUrlList)
        return urlList, uselessUrlList

    def textPreTreat(self, text):
        text = " ".join(re.findall('[\u4e00-\u9fa5]+', text))
        jieba.setLogLevel(jieba.logging.INFO)  # 去掉提示信息
        seg_list = jieba.cut(text)
        words = []
        for seg in seg_list:
            if (seg.isalpha()) & (seg not in self.stopwords):
                words.append(seg)
        sentence = " ".join(words)
        return sentence

    def transformData(self, urlList: list, uselessUrlList: list):
        with open(self.trainDataPath, mode="a+", encoding='utf-8') as f:
            f.write("type,text\n")
        twoUrlList = [urlList, uselessUrlList]
        tagList = [True, False]
        for i in range(len(twoUrlList)):
            for url in twoUrlList[i]:
                html = WebSpider.getHtml(url)
                if html:
                    html, soup = TableExtract.htmlPreTreat(html)
                    text = soup.text
                    sentence = self.textPreTreat(text)

                    with open(self.trainDataPath, mode="a+", encoding='utf-8') as f:
                        f.write(f"{tagList[i]},{sentence}\n")

    def train(self):
        dataFrame = pandas.read_csv(self.trainDataPath)
        textMatrix = self.transformTextToSparseMatrix(dataFrame["text"])
        features = pd.DataFrame(textMatrix.apply(sum, axis=0))
        extractedFeatures = [features.index[i] for i in range(features.shape[0]) if features.iloc[i, 0] > 20]
        FileIO.writePkl(self.charactersVocabularyPath, extractedFeatures)
        textMatrix = textMatrix[extractedFeatures]
        train, test, trainLabel, testLabel = train_test_split(textMatrix, dataFrame["type"], test_size=0.2)
        # train model
        clf = bayes.BernoulliNB(alpha=1, binarize=True)
        model = clf.fit(train, trainLabel)
        joblib.dump(clf, self.bayesModelPath)
        print(f"该模型的正确率：{model.score(test, testLabel)}，模型已经完成覆写")
        # clf.predict(X)

    def predictOneText(self, text):
        textMatrix = self.transformTextToSparseMatrix(pd.Series({'0': text}))
        extractedFeatures = FileIO.readPkl(self.charactersVocabularyPath)
        features = pd.DataFrame(textMatrix.apply(sum, axis=0))
        wordCharacterList = []
        indexSet = set(features.index)
        for feature in extractedFeatures:
            if feature in indexSet:
                wordCharacterList.append(features[0][feature])
            else:
                wordCharacterList.append(0)

        clf = joblib.load(self.bayesModelPath)
        res = clf.predict(array(wordCharacterList).reshape(1, -1))
        return res

    def predictUrl(self, url: str):
        html = WebSpider.getHtml(url)
        html = self.textPreTreat(html)
        res = self.predictOneText(html)
        return res

    @staticmethod
    def transformTextToSparseMatrix(texts):
        vectorizer = CountVectorizer(binary=False)
        vectorizer.fit(texts)

        # inspect vocabulary
        vocabulary = vectorizer.vocabulary_
        # print("There are ", len(vocabulary), " word features")

        vector = vectorizer.transform(texts)
        result = pd.DataFrame(vector.toarray())

        keys = []
        values = []
        for key, value in vectorizer.vocabulary_.items():
            keys.append(key)
            values.append(value)
        df = pd.DataFrame(data={"key": keys, "value": values})
        colnames = df.sort_values("value")["key"].values
        result.columns = colnames
        return result

    def test(self):
        # urlRecord = FileIO.readPkl(self.urlRecordPath)
        # url1 = list(urlRecord)[0]
        url1 = "https://baike.baidu.com/item/%E8%9B%8B%E7%99%BD%E8%B4%A8/309120"
        html1 = WebSpider.getHtml(url1)
        html1 = self.textPreTreat(html1)
        print(self.predictOneText(html1))

        # uselessUrlRecord = FileIO.readPkl(self.uselessUrlRecordPath)
        # url2 = list(uselessUrlRecord)[0]
        # html2 = WebSpider.getHtml(url2)
        # html2 = self.textPreTreat(html2)
        # print(self.predictOneText(html2))



