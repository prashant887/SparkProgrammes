import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{count,sum}
object SparkHiveApp {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("SparkHiveApp")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport().getOrCreate()

    import spark.implicits._
    val DeptDF=spark.sql("select * from retail_db.departments").toDF("department_id","department_name").as("dept")

    val OrdersDF=spark.sql("select * from retail_db.orders")
      .toDF("order_id","order_date","order_customer_id","order_status").as("orders")

    val OrderItems=spark.sql("select * from retail_db.order_items")
      .toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price").as("orderitems")

   /* DeptDF.show(truncate = false)

    OrdersDF.show(truncate = false)

    OrderItems.show(truncate = false)*/

    OrdersDF.join(OrderItems,$"orders.order_id"===$"orderitems.order_item_order_id")
        .groupBy($"orders.order_id")
      .agg(sum($"orderitems.order_item_product_price").alias("AggSum"),count("*").alias("AggCnt"))
        .show(truncate = false)

    spark.stop()

  }
}
