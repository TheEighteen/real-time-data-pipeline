package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import sys.process._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._

object obj1 {

  def main(args: Array[String]): Unit = {

    println("====started=======")

    val conf = new SparkConf()
      .setAppName("ES")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true") // you can remove this if cleanup works well

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(sc, Seconds(5))

    val topics = Array("test")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "first",
      "auto.offset.reset" -> "earliest"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

     val data = stream.map(record => record.value)
     data.print()

    data.foreachRDD(x =>
      if(!x.isEmpty())
        {
          val df1 = spark.read.option("multiline","true").json(x)
          /*df1.printSchema()
          df1.show()*/

          val df2 = df1.withColumn("results",expr("explode(results)"))
          /*df2.printSchema()
          df2.show()*/

          val finalDF = df2.select(
            "results.user.name.title",
            "results.user.name.first",
            "results.user.name.last",
            "results.user.gender",
            "results.user.dob",
            "results.user.email"
          )
          println("====Data Frame====")
          finalDF.show()

          finalDF.write.mode("overwrite").json("file:///C:/vinay/Kafka+Spark_Streaming")
        }
    )

    try {
      ssc.start()
      ssc.awaitTermination()
    } finally {
      println("====Ended=====")
      ssc.stop()
    }
  }

}

