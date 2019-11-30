import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext,SparkConf}

object AirLineExample {

  case class FlightKey(ORIGIN_AIRPORT_ID:Int,ORIGIN_AIRPORT_SEQ_ID:Int,ORIGIN_CITY_MARKET_ID:Int,
                       DEST_AIRPORT_ID:Int,DEST_AIRPORT_SEQ_ID:Int,DEST_CITY_MARKET_ID:Int)

  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(0).toInt, data(1).toInt,data(2).toInt,data(3).toInt,data(4).toInt,data(5).toInt )
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("AirLineExample").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext


    val RawRdd=sc.textFile("D:\\936479866_T_ONTIME_REPORTING.csv")
        .map(x=>{x.split(",")}).filter(x=>{x(0).matches("\\d+")})

    val keyedByData = RawRdd.keyBy(arr => createKey(arr))
    keyedByData.take(5).foreach(x=>{println(x._2.mkString(" "))})


    sc.stop()
    spark.stop()
  }
}
