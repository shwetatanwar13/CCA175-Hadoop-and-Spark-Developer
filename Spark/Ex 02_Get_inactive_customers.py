hdfs dfs -get /user/shwetatanwar13 /data/retail_db 

hdfs dfs -copyFromLocal /data/retail_db 

ordersRDD=sc.textFile("/user/shwetatanwar13/retail_db/orders") 
ordersRDD.take(5)
customersRDD=sc.textFile("/user/shwetatanwar13/retail_db/customers")
customersRDD.take(5)
orderMap=ordersRDD.map(lambda x:(x.split(',')[2],x.split(',')[0]))
orderMap.take(5)
customerMap=customersRDD.map(lambda x:(x.split(',')[0],(x.split(',')[1], x.split(',')[2])))
customerMap.take(5)
joinRDD=customerMap.leftOuterJoin(orderMap)
joinRDD.take(5)
filterRDD=joinRDD.filter(lambda x:x[1][1]==None)
filterRDD.take(5)

joinRDD.count()

joinRDD1=customerMap.join(orderMap)
joinRDD1.count()

finalRDD=joinRDD.subtract(joinRDD1).map(lambda x:(x[1][0][0],x[1][0][1])).sortByKey()

finalRDD.take(10)


ordersRDD=sc.textFile("/user/shwetatanwar13/retail_db/orders")

from pyspark.sql import Row

ordersRDD=sc.textFile("/user/shwetatanwar13/retail_db/orders") 
orderDF=ordersRDD.map(lambda x:Row(order_customer_id=x.split(',')[2])).toDF()
orderDF.registerTempTable("orders")

customersRDD=sc.textFile("/user/shwetatanwar13/retail_db/customers")
customerDF=customersRDD.map(lambda x:Row(customer_id=x.split(',')[0],customer_fname=x.split(',')[1],customer_lname=x.split(',')[2])).toDF()
customerDF.registerTempTable("customers")

finalRDD=sqlContext.sql("select customer_lname,customer_fname \
 from customers c left outer join orders o \
 on c.customer_id =o.order_customer_id \
 order by customer_lname,customer_fname")
 
finalRDD.write.save("/user/shwetatanwar13/solutions/solutions02/inactive_customers")

sqlContext.setConf("spark.sql.shuffle.partitions", "1")
finalRDD1=sqlContext.sql("select concat(customer_lname,',',customer_fname) \
 from customers c left outer join orders o \
 on c.customer_id =o.order_customer_id \
 order by customer_lname,customer_fname")
 
finalRDD1.write.text("/user/shwetatanwar13/solutions/solutions02/inactive_customers")

finalRDD.map(lambda r : r[0]+','+r[1]).coalesce(1).saveAsTextFile("/user/shwetatanwar13/solutions/solutions02/inactive_customers")
finalRDD.show()