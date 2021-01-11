from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import json

sc = SparkContext("local[*]", "IGDBTop")
ssc = StreamingContext(sc, 10)
ss = SparkSession.builder.config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()

KafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "gaming", {"igdbdata": 1})
TopGames = KafkaStream.flatMap(lambda x: json.loads(x[1]))

def process(rdd):
  if not rdd.isEmpty():
    print("Creating dataframe")
    global ss
    #{'id': 121756, 'first_release_date': 1578700800, 'genres': [5, 31], 'name': "Metro Exodus: Sam's Story", 'rating': 70.46817062715961}
    mySchema = StructType([StructField('id', IntegerType(), True), StructField('first_release_date', IntegerType(), True), StructField('genres', ArrayType(IntegerType(), True), True), StructField('name', StringType(), True), StructField('parent_game', IntegerType(), True), StructField('rating', FloatType(), True), StructField('hypes', IntegerType(), True), StructField('follows', IntegerType(), True)])
    dataframe = ss.createDataFrame(rdd, schema=mySchema)
    dataframe.show()
    dataframe.write.mode("overwrite").saveAsTable("igdbtables.topgames")
    print("Table saved to Hive")
  else:
    print("RDD EMPTY")

TopGames.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
