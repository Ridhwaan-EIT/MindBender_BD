from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from re import sub



sc = SparkContext("local[*]", "Shakespeare")
ssc = StreamingContext(sc, 3)

ss = SparkSession.builder \
	.enableHiveSupport() \
	.getOrCreate()


y = sc.textFile("/home/fieldemployee/Downloads/MindBender_BD/Task-001/Shakespeare.txt").flatMap(lambda line: line.split(" "))
	
	
x = y.map(lambda i: i.lower()) \
	.map(lambda i: sub('[^A-Za-z0-9]+', '', i)) \
	.map(lambda i: (i, 1)) \
	.reduceByKey(lambda a,b:a +b)
	


x.saveAsTextFile("/home/fieldemployee/Downloads/MindBender_BD/Task-011")




ssc.start()

ssc.awaitTermination()
