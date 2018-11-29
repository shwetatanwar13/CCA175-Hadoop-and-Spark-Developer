orderRDD=sc.textFile("/public/retail_db/orders")
orderRDD.take(5)
filterorderRDD=orderRDD.filter(lambda x:x.split(",")[3] in ("CLOSED","COMPLETE")). \
map(lambda x:(int(x.split(",")[0]),x.split(",")[1][:10]))


oiRDD=sc.textFile("/public/retail_db/order_items")
kvoiRDD=oiRDD.map(lambda x:(int(x.split(",")[1]),x))
orderitemRDD=filterorderRDD.join(kvoiRDD)

productrevRDD=orderitemRDD.map(lambda x:((x[1][0],int(x[1][1].split(',')[2])),float(x[1][1].split(',')[4])))


productdailyrevRDD=productrevRDD.reduceByKey(lambda x,y:x+y)
productdailyrevRDD=productdailyrevRDD.map(lambda x:(x[0][1],(x[0][0],x[1])))

product=open("/data/retail_db/products/part-00000")
productRDD=sc.parallelize(product)
productRDD=productRDD.map(lambda x:(int(x.split(',')[0]),x.split(',')[2]))
finalRDD=productdailyrevRDD.join(productRDD)
finalRDD=finalRDD.map(lambda x:((x[1][0][0],-round(x[1][0][1],2)),(round(x[1][0][1],2),x[1][1])))

sortedfinalRDD=finalRDD.sortByKey().map(lambda x:str(x[0][0])+','+str(x[1][0])+','+str(x[1][1]))
for i in sortedfinalRDD.take(50):print(i)


sortedfinalRDD.coalesce(2).saveAsTextFile("/user/shwetatanwar13/daily_revenue_txt_python2")

avrofinalRDD=finalRDD.sortByKey().map(lambda x:(x[0][0],x[1][0],x[1][1]))

for i in avrofinalRDD.take(10):print(i)

avroDF=avrofinalRDD.coalesce(2).toDF(["Date","Revenue","Product"])

avroDF.save("/user/shwetatanwar13/daily_revenue_avro_python1","com.databricks.spark.avro")
