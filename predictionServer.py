# /home/wangning/distribute/spark-prediction/bin/spark-submit --master local[1] predictionServer.py
from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession,Row
import tornado
from tornado.httpclient import HTTPClient,HTTPRequest
from tornado.web import RequestHandler
from tornado import gen,ioloop
from pyspark.ml.linalg import Vectors
import json
from hdfs.client import Client,InsecureClient
import subprocess
import re
import time

toparttern = '''『|』|:| |\t|\r|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|=|\[|\+|\{|\]|\}|\\|\||;|'|"|,|<|\.|>|/|\?|！|＃|¥|％|⋯⋯|—|＊|（|）|－|——|＝|＋|［|｛|］|｝|、|｜|；|：|‘|“|，|《|。|》|／|？|`|~|｀|～|”|\n'''
pattern = re.compile(toparttern)


# input : {"sent":"aaaa"}
# output : {"prediction":"500"}


spark = SparkSession.builder.appName("Prediction").getOrCreate() 
#mymodel = KMeansModel.load("/simisearch/model/kmeans-20180919-10000k") 
mymodel = KMeansModel.load("/simisearch/model/kmeans-20181023-10000k") 
serverUrl = 'http://194.169.1.75:8080/classficate/vector'
client = HTTPClient()
hdfsClient = InsecureClient("http://194.169.1.92:50070",user="hdfs",root="/simisearch/taskout")
schema = 'uuid,send_carrier_id,send_num,send_imsi,send_imei,send_homezip,send_visitzip,send_lac,send_ci,send_lon,send_lat,send_lon_lat,send_tmsi,send_old_tmsi,rece_carrier_id,rece_num,rece_imsi,rece_imei,rece_homezip,rece_visitzip,rece_lac,rece_ci,rece_lon,rece_lat,rece_lon_lat,rece_tmsi,rece_old_tmsi,call_type,start_time,msc_id,content,bsc_point_code,msc_point_code,total_sms_num,cur_num,sms_ref,merge_num,lang1,lang2,md5,rubbish,cat1,classify_name,confidence'
fields = schema.split(',')
class SaveDataToESHandler(RequestHandler):
    @gen.coroutine
    def post(self):
        paramsBody = json.loads(self.request.body)
        taskId = paramsBody["taskId"]
        dataFile = '/simisearch/taskout/' + taskId + "/part-00000"
        with hdfsClient.read(dataFile,length=10485760,encoding='utf-8') as reader:
            dataLines = reader.readlines()
        outputFile = '/mnt/data01/simisearchTaskout/' + taskId
        with open(outputFile,"w") as writer:
            count = 0
            for line in dataLines:
                values = line.strip().split(',')
                dataId = taskId + "-" + str(count)
                dataDict = dict()
                for i in range(len(fields)):
                    dataDict[fields[i]] = values[i]
                dataDict["taskId"] = taskId
                dataDict["start_time"] = str(time.mktime(time.strptime(dataDict["start_time"],"%Y%m%d%H%M%S")))[:-2] + "000"
                indexDict = {"index":{"_index": "classifyresult","_type":"doc","_id":dataId}}
                writer.write(json.dumps(indexDict) + "\n")
                writer.write(json.dumps(dataDict,ensure_ascii=False) + "\n")
                count +=1
                if count == 10000:
                    break
        postCmd = '''curl -H "Content-Type:application/json;charset='utf-8'" -XPOST '194.169.1.67:9200/_bulk?pretty' --data-binary @%s ''' %(outputFile)
        print(postCmd)
        subprocess.Popen(postCmd,shell=True,stdout=subprocess.PIPE).communicate()

class DeleteHdfsFileHandler(RequestHandler):
    @gen.coroutine
    def post(self):
        paramsBody = json.loads(self.request.body)
        taskId = paramsBody["taskId"]
        flag = hdfsClient.delete(taskId,recursive=True)
        self.write(json.dumps({"status":flag}))
        
class PredictionHandler(RequestHandler):
    @gen.coroutine
    def post(self):
        paramsBody = json.loads(self.request.body)
        print(paramsBody)
        content = paramsBody["sent"]
        content = re.sub(pattern, " ", content)
        paramsBody["sent"] = content
        request = HTTPRequest(url=serverUrl,method='POST',body=json.dumps(paramsBody))
        response = client.fetch(request)
        result = json.loads(response.body)["result"]
        vector = Vectors.dense(result.split(" "))
        
        rowData = [{"features":vector}]
        rowDF = spark.createDataFrame(rowData)
        
        transDF = mymodel.transform(rowDF)
        pre = transDF.collect()[0]["prediction"]
        preDict = {"prediction":str(pre)}
        print(preDict)
        self.write(preDict)
def make_app():
    return tornado.web.Application([
        (r"/classify/prediction",PredictionHandler),
        (r"/classify/deletehdfsfile",DeleteHdfsFileHandler),
        (r"/classify/savedatatoes",SaveDataToESHandler),
    ])

if __name__ == '__main__':
    app = make_app()
    app.listen(3100)
    ioloop.IOLoop.current().start()


