from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import json

sc = SparkContext("local[*]", "AeroDatabox")
ssc = StreamingContext(sc, 10)
ss = SparkSession.builder.config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()

KafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "American", {"flightdata": 1})
TopGames = KafkaStream.flatMap(lambda x: json.loads(x[1]))

def process(rdd):
  if not rdd.isEmpty():
    print("Creating dataframe")
    global ss
    
    mySchema = StructType([StructField('destination', ArrayType(StringType()), True),
					StructField('averageDailyFlights', StringType(), True),
					StructField('operators', ArrayType(StringType()), True)])    dataframe = ss.createDataFrame(rdd, schema=mySchema)
    dataframe.show()
    dataframe.write.mode("overwrite").saveAsTable("flightdata.KDFW")
    print("Table saved to Hive")
  else:
    print("RDD EMPTY")

TopGames.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
