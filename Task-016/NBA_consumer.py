from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json


def Process(rdd):
	if not rdd.isEmpty():
		global ss 
		df = ss.createDataFrame(rdd, schema=["id", "abbreviation",
			"city", "conference", "division", "full_name", "name"]).show()

		df.write.saveAsTable(name="SW",
			format="hive", mode="append")

sc = SparkContext("local[*]", "NBAData")
ssc = StreamingContext(sc, 7)

ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()

kafkastream = KafkaUtils.createStream(ssc, "localhost:2181", 
	"SW", {"SW": 1})

parsed = kafkastream.map(lambda x: json.loads(x[1]))

content = parsed.map(lambda x: x.get("content")) \
	.flatMap(lambda x: x.get("properties")) \
	.map(lambda x: (x.get("id"), x.get("abbreviation"), 
	x.get("city"), x.get("conference"), x.get("division"), 
	x.get("full_name"), x.get("name")))

filtered.foreachRDD(Process)


## Create a new DF based on NBA teams in the Southwest Division

hive_context = HiveContext(sc)
df = hive_context.table("default.SW")

new = df.select("id", "conference" "division") \
	.withColumn("city", "name") \
	.drop(col("full_name")) \
	.show()

ssc.start()
ssc.awaitTermination()
