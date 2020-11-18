import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
        .setAppName("Spark Count")
        .setMaster("local[*]"))
    
    val y = sc.textFile("/home/fieldemployee/Downloads/MindBender_BD/Task-001/Shakespeare.txt")
        .flatMap(line => line.split(" "))
        .map(word => (word, 1))

    var x = map.reduceByKey(_ + _)

    val sdata = y.flatMap(line => line.split(" "))
    val mdata = sdata.map(word => (word,1))
    val rdata = mdata.reduceByKey(_+_)

    x.saveAsTextFile("/home/fieldemployee/Downloads/MindBender_BD/Task-012")

    sc.stop()

  }
}

