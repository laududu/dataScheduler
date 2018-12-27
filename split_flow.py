from pyspark.sql import SparkSession
import datetime
import hashlib
import uuid
import re
import sys
import json
from hdfs.client import Client
client92 = Client(url="http://194.169.1.92:50070")
client93 = Client(url="http://194.169.1.93:50070")

try:
    client92.list("/")
    client = client92
except:
    client = client93

punctuation_Pattern = '''『|』|:| |\t|\r|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|=|\[|\+|\{|\]|\}|\\|\||;|'|"|,|<|\.|>|/|\?|！|＃|¥|％|⋯⋯|—|＊|（|）|－|——|＝|＋|［|｛|］|｝|、|｜|；|：|‘|“|，|《|。|》|／|？|`|~|｀|～|”|【|】|\n'''
zh_Pattern = re.compile(u'[\u4e00-\u9fa5]')
ja_Pattern = re.compile(u'[\u3040-\u309f]|[\u30a0-\u30ff]|[\u31f0-\u31ff]]')

def getData(chsms_list):
    for chsmsStr in chsms_list:
        chsms = chsmsStr.split(",")
        content = chsms[29]
        # 正则去符号
        content = re.sub(punctuation_Pattern, " ", content)
        # 正则判断中文
        if zh_Pattern.search(content) and not ja_Pattern.search(content):
            languageStr = "zh,zh"
        else:
            languageStr = 'other,other'

        # 判断是否垃圾短信
        sendNum = chsms[1]
        receNum = chsms[14]
        if ((len(sendNum) >= 3 and sendNum[:3] == "106") or (sendNum != "0" and len(sendNum) < 8) or (
                len(receNum) >= 3 and receNum[:3] == "106") or (receNum != "0" and len(receNum) < 8)):
            rubbish = "true"
        else:
            rubbish = "false"
        # 计算短信md5
        h = hashlib.md5()
        h.update(content.encode(encoding="utf-8"))
        md5Str = h.hexdigest()
        # 计算UUID
        uuidStr = str(uuid.uuid4())
        chsmsLine = uuidStr + "," + chsmsStr + "," + languageStr + "," + md5Str + "," + rubbish
        yield chsmsLine


today = datetime.datetime.now()
delta = datetime.timedelta(days=1)
yesterday = today - delta
begin = yesterday
end = yesterday

if len(sys.argv) > 1:
    startTime = sys.argv[1]
    endTime = sys.argv[2]
    begin = datetime.datetime.strptime(startTime, "%Y%m%d")
    end = datetime.datetime.strptime(endTime, "%Y%m%d")

day = begin
while day <= end:
    dayStr = day.strftime("%Y%m%d")
    day += delta
    # HDFS 输入输出目录
    hdfsInputPath = "/data/DW/merge/" + dayStr
    if client.content(hdfsInputPath,strict=False):
        spark = SparkSession.builder.appName("SplitFlow-" + dayStr).getOrCreate()
        sc = spark.sparkContext
        data = sc.textFile(hdfsInputPath)
        allData = data.mapPartitions(getData)

        hdfsOutputPath_zh = "/data/DW/splitflow_zh/" + dayStr
        zhData = allData.filter(lambda x: x.split(",")[37] == 'zh')
        zhData.repartition(100).saveAsTextFile(hdfsOutputPath_zh)

        hdfsOutputPath_other = "/data/DW/splitflow_other/" + dayStr
        otherData = allData.filter(lambda x: x.split(",")[37] == 'other')
        otherData.repartition(100).saveAsTextFile(hdfsOutputPath_other)

