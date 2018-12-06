from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

hostname=sys.argv[1]
port = int(sys.argv[2])
filePath = sys(argv[3])
conf=SparkConf(). \
setAppName("Streaming Department Count"). \
setMaster("yarn-client")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,30)

messages=ssc.socketTextStream(hostname,port)

deparFilter=messages. \
filter(lambda x:x.split()[6].startswith('/department')). \
map(lambda x:x.split()[6])

depCount=deparFilter. \
map(lambda x:(x.split('/')[2],1)).reduceByKey(lambda x,y:x+y)

depCount.saveAsTextFiles()

ssc.start()
ssc.awaitTermination()

