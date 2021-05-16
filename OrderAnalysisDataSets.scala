import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.log4j.{Level, Logger}

object OrderAnalysisDataSets {

  case class OrdersClass(order_id:Int, order_date:java.sql.Timestamp, order_customer_id:Int, order_status:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("OrderAnalysisDataSets").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val schema=Encoders.product[OrdersClass].schema
    val OrdersDs=spark.read.option("header","false")
      .format("csv")
      .option("timestampFormat","yyyy-mm-dd hh:mi:ss")
      .schema(schema = schema)
      .load("/Users/pl465j/Downloads/retail_db-master/orders")
      .toDF("order_id","order_date","order_customer_id","order_status")
      .as[OrdersClass]
      .as("Orders")

    OrdersDs.show(truncate = false)
    spark.stop()

  }
}
