import csv
import json
import pickle
import sys

from bs4 import Tag


class FileIO:
    def __init__(self, filepath):
        self.filepath = filepath

    @staticmethod
    def writePkl(filepath: str, data, mode='wb+') -> bool:
        try:
            with open(filepath, mode) as f:
                pickle.dump(data, f)
                return True
        except Exception as e:
            print("写入pkl文件失败", e)
            return False

    @staticmethod
    def readPkl(filepath: str, mode='rb+'):
        try:
            with open(filepath, mode=mode) as f:
                return pickle.load(f)
        except Exception as e:
            print("读取pkl文件失败", e)

    @staticmethod
    def writeList2Pkl(filepath: str, dataList: list):
        for data in dataList:
            try:
                with open(filepath, 'ab') as f:
                    pickle.dump(data, f)
            except Exception as e:
                print("列表写入pkl文件失败", e)

    @staticmethod
    def readPkl2List(filepath: str) -> list:
        resList = []
        try:
            with open(filepath, 'rb') as f:
                while True:
                    try:
                        data = pickle.load(f)
                        resList.append(data)
                    except EOFError:
                        break
        except Exception as e:
            print("读取pkl文件到列表失败", e)
        finally:
            return resList

    @staticmethod
    def writeTag2Html(filepath: str, tag: Tag):
        with open(
                filepath,
                mode="w+", encoding="utf-8") as f:
            f.write("""<!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body>
            <table data-sort="sortDisabled" log-set-param="table_view">""")
            f.write(str(tag))
            f.write("""</table>
            </body>
            </html>
                            """)

    @staticmethod
    def readHtmlFormFile(filepath: str) -> str:
        with open(filepath, mode='r', encoding='utf-8') as f:
            return f.read()

    @staticmethod
    def write2Json(data, filepath: str, mode="w+", changeLine=False):
        with open(filepath, mode, encoding="utf-8", ) as f:
            json.dump(data, f, ensure_ascii=False)
            if changeLine:
                f.write("\n")

    @staticmethod
    def readJson(filepath: str, mode="r+"):
        with open(filepath, mode, encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def writeTriad2csv(filepath: str, TriadList: list, mode="w"):
        with open(filepath, mode=mode, encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(TriadList)
