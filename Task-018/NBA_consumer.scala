import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object NBA_Consumer
{

    def main(args: Array[String])
    {

      val schema =
   		StructType(List(
     	StructField("city", StringType, true),
     	StructField("conference", StringType, true) ) )

   	  val schema2 =
   		StructType(List(
     	StructField("division", StringType, true),
     	StructField("abbreviation", StringType, true) ) )

   	  val schema3 =
   		StructType(List(
     	StructField("id", StringType, true),
     	StructField("full_name", StringType, true),
     	StructField("name", StringType, true) ) )

      val ss = SparkSession
      	.builder
      	.appName("NBA_consumer")
      	.master("local[*]")
      	.getOrCreate()

      import ss.implicits._

      val inputDf = ss
      	.readStream
      	.format("kafka")
      	.option("kafka.bootstrap.servers", "localhost:9099")
      	.option("subscribe", "coin")
      	.load()

	  val rawDataframe = inputDf.select($"value" cast "string" as "json")
	  	.select(from_json($"json", schema) as "stuff")
	  	.select("stuff.*")
	  	.drop("city")  

	  val coin = rawDataframe.select(from_json($"conference", schema2) as "stuff")
	  	.select("stuff.*")
	  	.drop("division")
	  	.select(from_json($"abbreviation", schema3) as "abbreviation")
	  	.select("abbreviation.*")

      val query = coin
      	.writeStream
      	.outputMode("Append")
      	.format("console")
      	.start()
      
      query.awaitTermination()

    }
}

