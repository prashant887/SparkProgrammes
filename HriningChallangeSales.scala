import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SparkSession
import  org.apache.spark.{SparkContext,SparkConf}
object HriningChallangeSales {

  case class ProductClass(ProductId:Int,ProductName:String,ProductPrice:Float)
  case class SalesClass(SaleId:Int,ProuductId:Int,SaleDate:String,units:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("HriningChallangeSales")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext

    val ProductRdd=sc.textFile("D:\\pharmaceutical_sale_sample\\Product.csv")
    val SaleRdd=sc.textFile("D:\\pharmaceutical_sale_sample\\Sale.csv")

     val Products=ProductRdd
         .map(x=>{x.split(",")})
       .filter(x=>{x(0).matches("\\d+")}) //To Filter Headrs or Non NumberalId
       .map(x=>{(x(0),(x(1),x(2)))})
    val Sales=SaleRdd.
      map(x=>{x.split(",")})
        .filter(x=>{x(0).matches("\\d+")}) //To Filter Headrs or Non NumberalId
        .map(x=>{(x(1),(x(2).substring(0,10).replace("-",""),x(3)))})
    val JoinRdd=Products.join(Sales)

    //JoinRdd.foreach(x=>{println(x)})

    //This is including price of Product Higest Sales
    val SelectedCols=JoinRdd.map(x=>{(x._2._1._1,x._2._1._2.toInt*x._2._2._2.toInt,x._2._2._1)}) //Multiple unit*price
        .map(x=>{((x._1,x._3),x._2)}) // get only product name and date as key , total cost as key
      .reduceByKey((x,y)=>{x+y}) //add prrice of same product and date
      .sortBy(x=>{(x._1._2,-x._2)}) //sort based on date and total
      .map(x=>{(x._1._2,x._1._1)}) // paired rdd where date is key and prooduts are key
        .groupByKey() // group by key to get in row
        .map(x=>{(x._1,x._2.mkString(","))}).collect() // get values in comma seperate
//    SelectedCols.foreach(x=>{println(x)})

    //This is based on Number of Units

    println("\n \n")
    val NumOfProducts=JoinRdd.map(x=>{(x._2._1._1,x._2._1._2.toInt,x._2._2._2.toInt,x._2._2._1)})
        .map(x=>{((x._4,x._1),x._3)}) //Paird Rdd where product and date as key and number of unit as key
        .reduceByKey((x,y)=>{x+y})
        .sortBy(x=>{(x._1._1,-x._2)})
        .map(x=>{(x._1._1,x._1._2)})
        .groupByKey()
        .map(x=>{(x._1,x._2.mkString(","))})
       // .collect()

    import spark.implicits._

    val dataFrame=NumOfProducts.toDF("day","top")

    dataFrame.show()
    sc.stop()
    spark.stop()
  }

}
