from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

def Process(rdd):
	if not rdd.isEmpty():
		global ss 
		#lines = filtered.foreachRDD(lambda rdd: rdd.toDF())
		df = ss.createDataFrame(rdd , schema=["id", "text"])
		df.show()

sc = SparkContext("local[*]", "TwitterData")
ssc = StreamingContext(sc, 10)

ss = SparkSession.builder.enableHiveSupport().getOrCreate()

kafkastream = KafkaUtils.createStream(ssc, "localhost:2181","tweets", {"tweets": 1})

parsed = kafkastream.map(lambda x: json.loads(x[1]))

filtered = parsed.filter(lambda x: x.get("lang") == "en").map(lambda x: (x.get("id"), x.get("text")))

#filtered2 = parsed.flatmap(lambda x: (x.get("friends_count"), x.get("screen_name")))
#filtered2.pprint()

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

filtered.foreachRDD(Process)

ssc.start()
ssc.awaitTermination()


