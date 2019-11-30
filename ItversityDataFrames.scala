import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.KafkaUtils
import  org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object ItversityDataFrames {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf()
      .setAppName("ItversityDataFrmae")
      .setMaster("local[*]")

    val spark= SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    val ordersRdd=sc.textFile("D:\\data-master\\data-master\\retail_db\\orders")

    import spark.implicits._
    val ordersDF=ordersRdd
      .map(x=>{x.split(",")})
      .map(x=>{(x(0).toInt,x(1).replace("00:00:00.0","").trim,x(2).toInt,x(3))})
      .toDF("order_id","order_date","order_customer_id","order_status")

    //val ordersDataFrame=spark.createDataFrame(ordersRdd,ordersDF.printSchema())

    ordersDF.show()
    ordersDF.printSchema()

    ordersDF.createOrReplaceTempView("orders")

    spark.sql("select order_id,order_date,order_customer_id,order_status from orders").show()

    val ProductsRdd=sc.textFile("D:\\data-master\\data-master\\retail_db\\products")

    val productsDF=ProductsRdd.map(x=>{x.split(",")})
      .map(x=>{(x(0).toInt,x(2))})
      .toDF("product_id","product_name")

    productsDF.printSchema()

    productsDF.createOrReplaceTempView("products")

    spark.sql("select product_id,product_name from products").show()

    val orderItemsRdd=sc.textFile("D:\\data-master\\data-master\\retail_db\\order_items")

    val orderItemDF=orderItemsRdd
      .map(x=>{x.split(",")})
      .map(x=>{(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt,x(4).toFloat,x(5).toFloat)})
      .toDF("order_item_id","order_id","product_id","order_qunatity","order_item_subtotal","product_price")

    orderItemDF.show()
    orderItemDF.printSchema()

    orderItemDF.createOrReplaceTempView("order_items")

    spark.sql("select * from order_items").show()

    val queryOut=spark.sql("select a.order_date,b.product_price,c.product_name from orders a,order_items b,products c where a.order_id=b.order_id and b.product_id=c.product_id")

    //queryOut.write.mode("append").insertInto()
    queryOut.write.format("csv")
      .save("D:\\queryout")
    sc.stop()
    spark.stop()

    val ssc=new StreamingContext(conf,Seconds(1))

val stream=FlumeUtils.createPollingStream(ssc,"localhost",8321)
    /* Flume Starts*/
    val flumemessage=stream.map(x=>{new String(x.event.getBody.array())})
    flumemessage.foreachRDD(x=>{x.foreach(x=>{println(x)})})
    /*Flume Ends */
    /* Nc Starts*/
    val ncmessage=ssc.socketTextStream("localhost",9099)
    ncmessage.foreachRDD(x=>{x.foreach(y=>{println(y)})})
    /* NC ends */

    val KafkaParams=Map[String,Object](
"bookstrap-server" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")

    val kafkastream=KafkaUtils.createDirectStream[String,String](
      ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParams = KafkaParams))

  }
}
