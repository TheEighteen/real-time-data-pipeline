package pack

import org.apache.spark._
import org.apache.spark.sql._
import sys.process._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._


object obj {

  var sc: SparkContext=_
  var ssc: StreamingContext=_

  def main(args:Array[String]):Unit={

    // Stop any existing contexts
    //StreamingContext.getActive.foreach(_.stop(stopSparkContext = true, stopGracefully = true))
    //SparkContext.getActive.foreach(_.stop())

    // Stop previous context manually if needed
    if (ssc != null) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }

    println("====started=======")

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")

    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    ssc = new StreamingContext(sc,Seconds(5))

    val topics = Array("test")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "first",
      "auto.offset.reset" -> "earliest"
      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value).print()

    ssc.start()
    ssc.awaitTermination()


  }

}
