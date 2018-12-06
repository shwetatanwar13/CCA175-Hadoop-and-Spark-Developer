from pyspark import SparkConf,SparkContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.streaming import StreamingContext
import sys

hostname=sys.argv[1]
port = int(sys.argv[2])
filePath = sys.argv[3]

conf=SparkConf().setAppName("Flume Deaprtment Count").setMaster("yarn-client")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,30)
addresses = [(hostname,port)]
flumeStream = FlumeUtils.createPollingStream(ssc,addresses)
messages = flumeStream.map(lambda msg: msg[1])
deparFilter=messages. \
filter(lambda x:x.split()[6].startswith('/department/')). \
map(lambda x:x.split()[6])
depCount=deparFilter. \
map(lambda x:(x.split('/')[2],1)).reduceByKey(lambda x,y:x+y)
depCount.pprint()
depCount.saveAsTextFiles(filePath)
ssc.start()
ssc.awaitTermination()
