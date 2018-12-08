from pyspark import SparkConf,SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import sys

filePath = sys.argv[3]

conf=SparkConf().setAppName("Kafka Department Count").setMaster("yarn-client")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,30)
topics = ["fltokf"]
brokerList = {"metadata.broker.list" : "wn01.itversity.com:6667,wn02.itversity.com:6667,wn03.itversity.com:6667,wn04.itversity.com:6667"}
directKafkaStream = KafkaUtils.createDirectStream(ssc, topics, brokerList)
messages = directKafkaStream.map(lambda msg: msg[1])
deparFilter=messages. \
filter(lambda x:x.split()[6].startswith('/department/')). \
map(lambda x:x.split()[6])
depCount=deparFilter. \
map(lambda x:(x.split('/')[2],1)).reduceByKey(lambda x,y:x+y)
depCount.pprint()
depCount.saveAsTextFiles(filePath)
ssc.start()
ssc.awaitTermination()
