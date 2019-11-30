import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkContext

object TopNPrices {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","TopNPrice")

    val products=sc.textFile("D:\\data-master\\data-master\\retail_db\\products")

    val productsMap=products
      .filter(x=>{x.split(",")(4)!=""})
      .map(x=>{(x.split(",")(1).toInt,x)})
      .groupByKey()

    val productIterable=productsMap.
      first()._2
      .map(x=>{x.split(",")(4).toFloat})
      .toSet
    val topNPrices=productIterable
      .toList
      .sortWith((x,y)=>{x>y})
      .take(5)

    val productSorted=productsMap
        .first()._2
        .toList
        .sortBy(x=>{-x.split(",")(4).toFloat})

    val minTopPrice=topNPrices.min

    println("\n minTopPrice: %f \n".format(minTopPrice))

    val topNProductPrices=productSorted.takeWhile(x=>{x.split(",")(4).toFloat>=minTopPrice})
    topNProductPrices.foreach(x=>{println(x)})
  }
}
