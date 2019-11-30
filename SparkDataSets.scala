import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
object SparkDataSets {

  case class Orders(OrderId:Long,OrderDate:String,OrderItem:Long,OrderStatus:String)
  case class Rating(userId:Int,movieId:Int,rating:Double,timestamp:Long)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("SparkDataFrame")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext

    import spark.implicits._

    val columnNames = classOf[Orders].getDeclaredFields.map(x => x.getName)
println(columnNames.mkString(","))
    val OrdersDataSet=spark.read
        .option("nullValue","NA")
        .option("mode","failfast")
        .option("timestampFormat","YYYYMMDD")
      .option("inferSchema",true)
      .option("header", false)
        .csv("D:\\data-master\\data-master\\retail_db\\orders")
     // .toDF(columnNames:_*)
      .toDF("OrderId","OrderDate","OrderItem","OrderStatus")
        .as[Orders]

    OrdersDataSet.filter(x=>{x.OrderStatus.equalsIgnoreCase("COMPLETE")}).show()

    val MovieRatingRDD=spark.read
        .option("inferSchema",true)
      .option("header",true)
      .csv("D:\\ml-20m\\ml-20m\\ratings.csv")
        .as[Rating]

    MovieRatingRDD.filter(x=>{x.rating>4}).show()


    sc.stop()
    spark.stop()
  }
}
