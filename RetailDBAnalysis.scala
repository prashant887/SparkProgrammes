import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession

object RetailDBAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf=new SparkConf()
      .setAppName("RetailDBAnalysis")
      .setMaster("local[*]")

    val sc=new SparkContext(config=conf)

    val OrdersRdd=sc.textFile("D:\\data-master\\data-master\\retail_db\\orders")
    val OrderItems=sc.textFile("D:\\data-master\\data-master\\retail_db\\order_items")

    val OutputOrders=OrdersRdd.map(x=>{x.split(",")}).map(x=>{(x(0),x(3))})

    val orderItemsOp=OrderItems
      .map(x=>{x.split(",")})
      .map(x=>{(x(1).toInt,x(4).toFloat)})
      .groupByKey()
     // .mapValues(x=>{x.toList.sortWith((x,y)=>{x>y}).mkString(",")})
        .flatMapValues(x=>{x.toList.sortWith((x,y)=>{x<y})})
        .sortByKey()


    //orderItemsOp.foreach(x=>{println(x)})

    /* OutPut should be (key(int),(total(float),max(float)))
    * ReduceByKey for input (Int,Float) gives Output (Int,Float)
    * aggrgrateByKey for input(Int,Float) can give output (Int,(float,float))
    * intermedate fun (op,op,key)
    * */
    val revenueAndMaxPerProductId=OrderItems
      .map(x=>{x.split(",")})
      .map(x=>{(x(1).toInt,x(4).toFloat)})
      .filter(x=>{x._1==65722||x._1==65721})
        .aggregateByKey((0.0f,0.0f))(
          (itr,subtotal)=>{
            println("itr._1=%f + subtotal=%f ,itr._2=%f".format(itr._1,subtotal,itr._2))
            (itr._1+subtotal,if(subtotal>itr._2 ) subtotal else itr._2)},
          (total,inter)=>{
            println("total=%f %f inter=%f %f".format(total._1,total._2,inter._1,inter._2))
            (total._1+inter._1,if (total._2>inter._2) total._2 else inter._2)
          }
        )

    revenueAndMaxPerProductId.foreach(x=>{println(x)})
  }
}
