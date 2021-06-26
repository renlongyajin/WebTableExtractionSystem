# WebTableExtractionSystem
这是一个基于百度百科的web表格抽取系统

该系统可以做到自动爬取百度百科网页，并从Web表格中抽取人物实体和关系，自动构建知识图谱，并存储到neoj数据库中。原理上可以遍历所有的百度百科页面。

详细说明文档以及PPT，需要下载项目中file文件夹下的“说明书”文件夹查看。以下为复制粘贴版本，不带有图片。仅仅包含配置说明。

1. 安装环境
windows10系统

python3.6.5环境

Microsoft SQL Server Management Studio 17（community版即可）

JAVA环境：x64的jre1.8.0_241

neo4j-community-3.5.14

python库：详情见requirements.txt文件

1.1. python环境创建
本项目使用了python3.6.5

本文使用pycharm的集成开发环境。同时配置了anaconda环境，使用anaconda环境创建了python3.6.5环境。

使用了如下语句进行了创建

conda create -n python3.6.5 python=3.6.5
1.2. requirements.txt
requests==2.25.1
numpy==1.19.5
pyodbc==4.0.0
joblib==1.0.1
pybloom_live==3.0.0
jieba==0.39
python_docx==0.8.10
pandas==0.23.4
ltp==4.1.3.post1
pyhanlp==0.1.77
treelib==1.6.1
pymssql==2.1.5
pyecharts==1.9.0
py2neo==3
beautifulsoup4==4.9.3
docx==0.2.4
PyQt5==5.15.4
scikit_learn==0.24.2
该文件也可以在项目中找到，可以使用如下语句一键安装所需要的环境：

pip install -r requirements.txt
注意：本文件中有一些库已经很久没有维护和更新，所以需要手动安装，当发现安装不了时，需要去网上寻找有关资源进行安装。

1.3. sqlserver数据库创建说明
使用以下代码创建数据库以及表格。

CREATE DATABASE WebTable;
GO
use Webtable;
CREATE TABLE pendingUrl(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[url] NTEXT NOT NULL
)

CREATE TABLE personUrlAndHtml(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[url] TEXT NOT NULL,
[html] NTEXT NOT NULL
)

CREATE TABLE entityAndRelationship(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[entity] NTEXT,
[relationship] NTEXT
)

CREATE TABLE uselessUrl(
[ID] BIGINT PRIMARY KEY IDENTITY(1,1),
[url] NTEXT NOT NULL
)

GO
以上语句会创建4个数据表。

Image 030621 123938.984
其中pendingUrl是指待处理的URL队列，该表中的URL链接必定是人物相关的链接。

而uselessUrl是指无用的URL链接，该表中的链接是与人物无关的链接。

personUrlAndHtml中记录的是url和当前url对应的html串，接下来表格信息抽取系统，会对这个html串抽取其中的表格。

而entityAndRelationship则用于存储实体和关系，其中entity是实体，relationship是关系。这其中，ID是BIGINT类型，其余的全部都是NTEXT类型。

1.4. 修改数据库连接语句
1.4.1. sqlserver数据库
IO文件夹中databaseInteraction文件夹中的MSSQL.py文件

需要修改数据库连接语句：

其中{}内即为要修改的数据

class SqlServerProcessor:
    def __init__(self, server: str = r"{数据库的IP地址}",
                 user: str = r"{用户名}",
                 password: str = "{密码}",
                 database: str = "WebTable"):
需要连接本地数据库，可以按照 这个链接 的说明进行配置。

注意：如果要通过TCP/IP协议连接sqlserver数据库，则需要先打开sqlserver数据库的TCP/IP服务。详情参考 这个链接。

1.4.2. neo4j数据库
knowledgeStorage文件夹中pesonGraph.py文件

需要修改数据库连接语句：

其中{}即为要修改的数据

    def __init__(self, host: str = "http://localhost:7474",
                 username: str = "{用户名}",
                 password: str = "{密码}"):
        """
        初始化人物图谱，链接到neo4j图数据库
        :param host: 连接地址
        :param username: 用户名，默认为neo4j
        :param password: 密码,默认为neo4j
        """
neo4j数据库的安装教程可以参考 这个链接

安装neo4j以及所需要的JRE环境的百度云链接： 链接：https://pan.baidu.com/s/19KXZjv2agBy4xtisxjSJKg 提取码：3cb6

下载好neo4j数据库后，若要运行程序，必须开启neo4j数据库
