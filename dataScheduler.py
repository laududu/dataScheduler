import sys
import subprocess
import datetime
import hdfs
from hdfs.client import Client,InsecureClient
import json
import os
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(filename="/mnt/data01/dataScheduler/run.log",level=logging.INFO,format=LOG_FORMAT,datefmt=DATE_FORMAT)

client92 = Client(url="http://194.169.1.92:50070")
client93 = Client(url="http://194.169.1.93:50070")

try:
    client92.list("/")
    client = client92
except:
    client = client93


CMD_MoveToHDFS = "hdfs dfs -moveFromLocal /mnt/data01/dataScheduler/smdrData/%s /data/DW/SMDR_PARQUET/%stemp"
CMD_Merge = '''/mnt/data01/dataScheduler/spark-yarn/bin/spark-submit --deploy-mode client --num-executors 50 --executor-cores 1 --executor-memory 4g --class wangning.spark.Merge /mnt/data01/dataScheduler/Merge.jar %s'''
CMD_LangClassify = '''/mnt/data01/dataScheduler/distribute/spark-yarn/bin/spark-submit --deploy-mode client --num-executors 50 --executor-cores 1 --executor-memory 4g --conf spark.pyspark.python="/opt/python3/bin/python" --conf spark.pyspark.driver.python="/mnt/data01/dataScheduler/distribute/python3/bin/python" --files /mnt/data01/dataScheduler/wiki.zh.bin,/mnt/data01/dataScheduler/lid.176.bin /mnt/data01/dataScheduler/lang_classify.py %s %s '''
CMD_Snapshot = '''/mnt/data01/dataScheduler/spark-yarn/bin/spark-submit --num-executors 20 --executor-cores 1 --executor-memory 4g --name es_smdr_%s --files /mnt/data01/dataScheduler/distribute/hadoop-2.7.2/etc/hadoop/core-site.xml,/mnt/data01/dataScheduler/distribute/hadoop-2.7.2/etc/hadoop/hdfs-site.xml --class org.elasticsearch.sfck.cluster.tasks.IndexTask /mnt/data01/dataScheduler/es-index-6.2.2-SNAPSHOT.jar /data/DW/lang_classify/%s /data/DW/snapshot_err/%s /data/DW/snapshot snapshotsmdr 20 smdr doc dw smdr '194.169.1.93:8010' '188.188.188.75:9200' %s false csv '''
CMD_InsertORC = ''' /mnt/data01/dataScheduler/distribute/spark-yarn/bin/spark-submit --deploy-mode client --name InsertORC --num-executors 50 --executor-cores 1 --executor-memory 4g --class wangning.spark.InsertORC /mnt/data01/dataScheduler/InsertORC.jar %s'''
CMD_AlterTable = '''beeline -u jdbc:hive2://idl93:10000 -e "alter table default.smdr%s add partition(day_time=%s);" '''
CMD_Splitflow = '''/mnt/data01/dataScheduler/spark-yarn/bin/spark-submit --deploy-mode client --num-executors 50 --executor-cores 1 --executor-memory 4g --conf spark.pyspark.python="/opt/python3/bin/python" --conf spark.pyspark.driver.python="/mnt/data01/dataScheduler/distribute/python3/bin/python" /mnt/data01/dataScheduler/split_flow.py %s %s '''
CMD_Snapshot_zh = '''/mnt/data01/dataScheduler/spark-yarn/bin/spark-submit --num-executors 20 --executor-cores 1 --executor-memory 4g --name es_smdr_%s --files /mnt/data01/dataScheduler/distribute/hadoop-2.7.2/etc/hadoop/core-site.xml,/mnt/data01/dataScheduler/distribute/hadoop-2.7.2/etc/hadoop/hdfs-site.xml --class org.elasticsearch.sfck.cluster.tasks.IndexTask /mnt/data01/dataScheduler/es-index-6.2.2-SNAPSHOT.jar /data/DW/splitflow_zh/%s /data/DW/snapshot_zh_err/%s /data/DW/snapshot_zh snapshotsmdr 20 smdr doc dw smdr '194.169.1.93:8010' '188.188.188.75:9200' %s false csv '''
CMD_Chown = '''hdfs dfs -chown -R elasticsearch:elasticsearch /data/DW/snapshot/%s '''
CMD_RecoverIndex = '''python /mnt/data01/dataScheduler/esRecover.py %s %s'''
def runCmd(cmd):
    logging.info(cmd)
    (result,error) = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    #logging.info("cmd result is " + str(result) + str(error))

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
    logging.info("*** %s ***" % (dayStr))
    localPath = "/mnt/data01/dataScheduler/smdrData/" + dayStr
    dataPath = "/data/DW/SMDR_PARQUET/%stemp" % (dayStr)
    mergePath = "/data/DW/merge/%s/_SUCCESS" % (dayStr)
    lang_classifyPath = "/data/DW/lang_classify/%s/_SUCCESS" % (dayStr)
    snapshotPath = "/data/DW/snapshot/%s" %(dayStr)
    snapshot_zhPath = "/data/DW/snapshot_zh/%s" %(dayStr)
    orcPath = "/inceptor1/user/hive/warehouse/default.db/hive/smdr%s/day_time=%s/_SUCCESS" % (dayStr[:4],dayStr)
    splitflowPath = "/data/DW/splitflow_zh/%s/_SUCCESS" % (dayStr)
    if os.path.exists(localPath):
        cmd_1 = CMD_MoveToHDFS % (dayStr,dayStr)
        runCmd(cmd_1)
    else:
        logging.error("scp from 205 is wrong")
        continue
    try: 
        client.content(dataPath)
        cmd_2 = CMD_Merge % (dayStr)
        runCmd(cmd_2)
    except:
        logging.error("push to hdfs is wrong")
        continue
    try:
        client.content(mergePath)
        cmd_3 = CMD_LangClassify % (dayStr,dayStr)
        runCmd(cmd_3)
    except:
        logging.error("merge is wrong")
        continue
    try:
        client.content(lang_classifyPath)
        cmd_4 = CMD_Snapshot % (dayStr,dayStr,dayStr,dayStr)
        runCmd(cmd_4)
    except:
        logging.error("lang_classify is wrong")
        continue
    try:
        client.content(snapshotPath)
    except:
        logging.error("snapshot is wrong")
        continue
    try:
        client.content(dataPath)
        cmd_5 = CMD_InsertORC % (dayStr)
        runCmd(cmd_5)
    except:
        logging.error("push to hdfs is wrong")
        continue
    try:
        client.content(orcPath)
    except:
        logging.error("insertORC is wrong")
        continue
    try:
        client.content(dataPath)
        cmd_5 = CMD_AlterTable % (dayStr[:4],dayStr)
        runCmd(cmd_5)
    except:
        logging.error("alterTable is wrong")
        continue
    try:
        client.content(mergePath)
        cmd_6 = CMD_Splitflow % (dayStr,dayStr)
        runCmd(cmd_6)
    except:
        logging.error("merge is wrong")
        continue
    try:
        client.content(splitflowPath)
    except:
        logging.error("splitflow is wrong")
        continue
    cmd_7 = CMD_Chown % (dayStr)
    runCmd(cmd_7)
#    cmd_7 = CMD_RecoverIndex % (dayStr,dayStr)
#    runCmd(cmd_7)
#    try:
#        client.content(splitflowPath)
#        cmd_4 = CMD_Snapshot_zh % (dayStr,dayStr,dayStr,dayStr)
#        runCmd(cmd_4)
#    except:
#        logging.error("lang_classify is wrong")
#        continue
#    try:
#        client.content(snapshot_zhPath)
#    except:
#        logging.error("snapshot_zh is wrong")
#        continue
