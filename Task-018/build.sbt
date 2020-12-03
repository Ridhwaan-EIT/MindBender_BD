name := "NBA_consumer"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(

    "org.apache.spark" %% "spark-core" % sparkVersion,

    "org.apache.spark" %% "spark-sql" % sparkVersion,

    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

)
