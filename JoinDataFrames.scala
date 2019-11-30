import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField,StructType,IntegerType,StringType,FloatType,TimestampType}
import org.apache.spark.sql.functions.{sum}

object JoinDataFrames {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("JoinDataFrame")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    import spark.implicits._

    val ordersSchema=StructType(Seq(
     StructField("order_id",IntegerType,false),
      StructField("order_date",TimestampType,false),
      StructField("order_customer_id",IntegerType,false),
      StructField("order_status",StringType,false)))

    val orderItemsSchema=StructType(Seq(
      StructField("order_item_id",IntegerType,false),
      StructField("order_item_order_id",IntegerType,false),
      StructField("order_item_product_id",IntegerType,false),
      StructField("order_item_quantity",IntegerType,false),
      StructField("order_item_subtotal",FloatType,false),
        StructField("order_item_product_price",FloatType,false)
    ))

    val OrdersDF=spark.read.format("csv")
        .schema(ordersSchema)
        .option("mode","failfast")
        .option("header","false")
        .option("nullValue","NA")
        .option("timestampFormat","yyyy-MM-dd HH:mm:ss.S")
        .option("path","D:\\data-master\\data-master\\retail_db\\orders")
        .load().as("orders")

    val OrdersItemsDF=spark.read.format("csv")
        .schema(orderItemsSchema)
        .option("header","false")
        .option("mode","failfast")
        .option("nullValue","NA")
        .option("path","D:\\data-master\\data-master\\retail_db\\order_items")
        .load().as("OrderItems")

    OrdersDF.show()
    OrdersItemsDF.show()

    val JoinedDF=OrdersDF
      .join(OrdersItemsDF,$"orders.order_id"===$"OrderItems.order_item_order_id","left")
        .select($"order_date",$"order_status",$"order_item_subtotal")
    JoinedDF.explain()

    JoinedDF.groupBy($"order_date",$"order_status")
      .agg(sum($"order_item_subtotal"))
      .withColumnRenamed("sum(order_item_subtotal)","TotalSales")
      .sort($"order_date",$"TotalSales".desc)
        .show()

    spark.catalog.listFunctions.filter('name like "%upper%").show()

    OrdersItemsDF.printSchema()
    sc.stop()
    spark.stop()
  }
}
