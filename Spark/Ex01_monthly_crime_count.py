Details - Duration 40 minutes

Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - ","
Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order

Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip


pyspark \
--master yarn \
--num-executors 6 \
--driver-memory 2g \
--executor-cores 2


07/24/2007 10:11:00 PM

from pyspark.sql import Row
crimesRDD=sc.textFile("/public/crime/csv")
crimesRDD.take(5)
crimesRDD1=crimesRDD.filter(lambda x:x.startswith('ID,Case Number'))
crimesfinalRDD=crimesRDD.subtract(crimesRDD1)


crimeDF=crimesfinalRDD.map(lambda x:Row(crime_year=x.split(',')[2].split('/')[2][:4],crime_month=x.split(',')[2].split('/')[0],crime_type=x.split(',')[5])).toDF()

crimeDF.show()

crimeDF.registerTempTable("crimes")

crimebymonthDF=sqlContext.sql("select concat(concat(crime_year,crime_month),'\t',count(*),'\t',crime_type) \
from crimes \
group by concat(crime_year,crime_month),crime_type \
order by concat(crime_year,crime_month) , count(*) desc")

crimebymonthDF.show()

crimebymonthDF.coalesce(1).write.option("codec", "org.apache.hadoop.io.compress.GzipCodec"). \
text("/user/shwetatanwar13/solutions/solution01/crimes_by_type_by_month")

/user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month


**********************************************************************************

Using RDD
crimesRDD=sc.textFile("/public/crime/csv")
crimesRDD.take(5)
crimesRDD1=crimesRDD.filter(lambda x:x.startswith('ID,Case Number'))
crimeDF=crimesfinalRDD.map(lambda x:((x.split(',')[2].split('/')[0],x.split(',')[5]),x.split(',')[2].split('/')[2][:4]))


