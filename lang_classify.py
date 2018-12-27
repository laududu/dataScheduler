from pyspark.ml.clustering import KMeans,KMeansModel
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import datetime
import hashlib
import uuid
import re
import sys
import os


def getAllDataAndVector(smdr_list):
    import fastText
    import numpy as np
    languageModelPath = 'lid.176.bin'
    languageModel = fastText.load_model(languageModelPath)
    classifyModelPath = 'wiki.zh.bin'
    classifyModel = fastText.load_model(classifyModelPath)
    for chsms in smdr_list:
        chsmsList = chsms.split(',')
        content = chsmsList[29]
        if len(content) > 8:
            # 正则去符号
            content = re.sub(pattern, " ", content)
            # 判断是否垃圾短信
            sendNum = chsmsList[1]
            receNum = chsmsList[14]
            rubbish = ""
            if ((len(sendNum) >= 3 and sendNum[:3] == "106") or (sendNum != "0" and len(sendNum) < 8) or (len(receNum) >= 3 and receNum[:3] == "106") or (receNum != "0" and len(receNum) < 8)):
                rubbish = "true"
            else:
                rubbish = "false"
            # 计算短信md5
            h = hashlib.md5()
            h.update(content.encode(encoding="utf-8"))
            md5Str = h.hexdigest()
            # 计算短信向量
            content_vector_array = classifyModel.get_sentence_vector(content)
            content_vector_list = content_vector_array.tolist()
            content_vector_str_list = [str(str_vec) for str_vec in content_vector_list]
            # 计算语种
            label, _ = languageModel.predict(content, k=2)
            label = [language.replace('__label__', '') for language in label]
            if len(label) < 2:
                label += ['unknown']
            languageStr = ",".join(label)
            # 计算UUID
            uuidStr = str(uuid.uuid4())
            chsms = uuidStr + "," + chsms + "," + languageStr + "," + md5Str + "," + rubbish
            yield [chsms,Vectors.dense(content_vector_str_list)]


def getOtherData(smdr_list):
    import fastText
    languageModelPath = 'lid.176.bin'
    languageModel = fastText.load_model(languageModelPath)
    for chsms in smdr_list:
        chsmsList = chsms.split(',')
        content = chsmsList[29]
        if len(content) > 8:
            continue
        else:
            # 正则去符号
            content = re.sub(pattern, " ", content)
            # 判断是否垃圾短信
            sendNum = chsmsList[1]
            receNum = chsmsList[14]
            rubbish = ""
            if ((len(sendNum) >= 3 and sendNum[:3] == "106") or (sendNum != "0" and len(sendNum) < 8) or (len(receNum) >= 3 and receNum[:3] == "106") or (receNum != "0" and len(receNum) < 8)):
                rubbish = "true"
            else:
                rubbish = "false"
            # 计算短信md5
            h = hashlib.md5()
            h.update(content.encode(encoding="utf-8"))
            md5Str = h.hexdigest()
            # 计算语种
            label, _ = languageModel.predict(content, k=2)
            label = [language.replace('__label__', '') for language in label]
            if len(label) < 2:
                label += ['unknown']
            languageStr = ",".join(label)
            # 计算UUID
            uuidStr = str(uuid.uuid4())
            chsms = uuidStr + "," + chsms + "," + languageStr + "," + md5Str + "," + rubbish
            # 返回其他类型数据和-1
            yield chsms + "," + "-1"



today = datetime.datetime.now()
delta = datetime.timedelta(days=1)
yesterday = today - delta
begin = yesterday
end = yesterday

if len(sys.argv) > 1:
    startTime = sys.argv[1]
    endTime = sys.argv[2]
    begin = datetime.datetime.strptime(startTime,"%Y%m%d")
    end = datetime.datetime.strptime(endTime,"%Y%m%d")

day = begin

while day <= end:
    dayStr = day.strftime("%Y%m%d")
    day += delta
 
    # HDFS 输入输出目录
    hdfsInputPath = "/data/DW/merge/" + dayStr
    hdfsOutputPath = "/data/DW/lang_classify/" + dayStr
    # 清洗短信内容的标点符号
    toparttern = '''『|』|:| |\t|\r|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|=|\[|\+|\{|\]|\}|\\|\||;|'|"|,|<|\.|>|/|\?|！|＃|¥|％|⋯⋯|—|＊|（|）|－|——|＝|＋|［|｛|］|｝|、|｜|；|：|‘|“|，|《|。||？|`|~|｀|～|”|\n'''
    pattern = re.compile(toparttern)
    spark = SparkSession.builder.appName("LanguageClassify-" + dayStr).getOrCreate()
    sc = spark.sparkContext
    data = sc.textFile(hdfsInputPath)
    data = data.coalesce(200)
    data.cache()
    data.count()
    allDataAndVector = data.mapPartitions(getAllDataAndVector)
    schema = ["allFields","features"]
    allFieldsDF = spark.createDataFrame(allDataAndVector,schema)
    otherData = data.mapPartitions(getOtherData)
    model = KMeansModel.load("/simisearch/model/kmeans-20181013-10000k")
    transformDF = model.transform(allFieldsDF)
    data_prediction = transformDF.select("allFields","prediction")
    pre_rdd = data_prediction.rdd.map(lambda x: x.allFields + "," + str(x.prediction))
    all_rdd = pre_rdd.union(otherData)
    all_rdd.cache()
    all_rdd.count()
    all_rdd.repartition(200).saveAsTextFile(hdfsOutputPath)
    all_rdd.unpersist()
    data.unpersist()


