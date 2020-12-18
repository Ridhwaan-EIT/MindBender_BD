from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F 
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.types import StructType, StructField
import json


## Define schema

x = StructType([StructField('icao', StringType(), True),
					StructField('destination', ArrayType(StringType()), True),
					StructField('averageDailyFlights', StringType(), True),
					StructField('operators', ArrayType(StringType()), True)])

## Create RDD, transform to Df and send to Hive

def Process(rdd):
	if not rdd.isEmpty():
		global ss 
		df = ss.createDataFrame(rdd, x).show()

		df.write.saveAsTable(name="kdfw",
			format="hive", mode="append")

## Create the SparkContext, StreamingContext, and SparkSession	

sc = SparkContext("local[*]", "AeroDatabox_KDFW")
ssc = StreamingContext(sc, 7)

ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()

## Create Kafka createStream object	

kafkastream = KafkaUtils.createStream(ssc, "localhost:2181", 
	"BD", {"BD": 1})

## Dump json from producer

parsed = kafkastream.map(lambda x: json.loads(x[1]))

## Apply mappings to get significant fields

content = parsed.flatMap(lambda x: x.get("properties")) \
	.map(lambda x: (x.get("destination"), x.get("averageDailyFlights"), x.get("operators")))

content.foreachRDD(Process)


## Create a new DF based on destination

hive_context = HiveContext(sc)
df = hive_context.table("default.BD")

new = df.select("averageDailyFlights" "operators") \
	.withColumn("destination") \
	.show()

ssc.start()
ssc.awaitTermination()
