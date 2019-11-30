import org.apache.spark.{SparkContext,SparkConf}
import org.apache.log4j.{Logger,Level}
import scala.io.Source
object ItVesityOrdersAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("ItVersityOrdersAnylsis")

    val sc=new SparkContext(config = conf)
    val orders=sc.textFile("D:\\data-master\\data-master\\retail_db\\orders")
    val orderItems=sc.textFile("D:\\data-master\\data-master\\retail_db\\order_items")

    val ordersCompleted=orders
      .filter(x=>{x.split(",")(3).equalsIgnoreCase("CLOSED")||x.split(",")(3).equalsIgnoreCase("COMPLETE")})

    val OrdersCompletedMap=ordersCompleted
        .map(x=>{x.split(",")})
        .map(x=>{(x(0).toInt,x(1).replace("00:00:00.0","").trim)})
    val OrderItemMap=orderItems
      .map(x=>{x.split(",")})
      .map(x=>{(x(1).toInt,(x(2).toInt,x(4).toFloat))})

    val ordersJoin=OrdersCompletedMap.join(OrderItemMap)

    val ProductIdMap = ordersJoin
      .map(x=>{((x._2._1,x._2._2._1),x._2._2._2)})


    val ProductsRaw=Source
      .fromFile("D:\\data-master\\data-master\\retail_db\\products\\part-00000")
      .getLines()



    var ProductMap:Map[Int,String]=Map()

    for(productFileds <- ProductsRaw){
val fileds=productFileds.split(",")
      ProductMap+= (fileds(0).toInt -> fileds(2))
    }

    val TotalProductSum=ProductIdMap
      .reduceByKey((x,y)=>{x+y})
        .map(x=>{(ProductMap.getOrElse(x._1._2,"Not Avliable"),x._1._1,x._2)})
        .sortBy(x=>{(x._2,-x._3)})
        .map(x=>{x._1+","+x._2+","+x._3})

    TotalProductSum.take(10).foreach(x=>{println(x)})



    sc.stop()

  }
}
