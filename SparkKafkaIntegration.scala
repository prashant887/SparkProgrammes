import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.JavaConverters._

object SparkKafkaIntegration {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("SparkKafkaIntegration").setMaster("local[*]")

    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

   val sc = new SparkContext(conf)
import spark.implicits._
    val kafkaParams=Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkKafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val offsetRanges = Array(
      OffsetRange("test", 0, 0, 100),
        OffsetRange("test", 1, 0, 100)

    )

   // val rdd = KafkaUtils.createRDD[String,String](sc , kafkaParams, Array(OffsetRange("test", 0, 0, 0)), PreferConsistent)

    sc.stop()

    val ssc=new StreamingContext(conf,Seconds(10))


    val topics=Array("test")
    val KafkaStream=KafkaUtils.createDirectStream[String,String](
      ssc,PreferConsistent,Subscribe[String,String](topics = topics,kafkaParams = kafkaParams)
    )

    val MessageOut=KafkaStream.map(x=>{(x.key(),x.value())})


    //  MessageOut.foreachRDD(x=>{x.collect().foreach(y=>{println(y._1+","+y._2)})})

   // MessageOut.foreachRDD(x=>{x.collect().foreach(y=>{println(y._2)})})



    val WordCount=KafkaStream.map(x=>{x.value()})
      .flatMap(x=>{x.split(" ")})
        .map(x=>{(x,1)})
        .reduceByKey((x,y)=>{x+y})

    WordCount.print()
    ssc.start()
    ssc.awaitTermination()
    spark.stop()
  }
}
