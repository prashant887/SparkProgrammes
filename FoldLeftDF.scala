import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit}
object FoldLeftDF {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("FoldLeftDF")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val ColSeq=List[String]("A","B","C")
    val values = List(1,2,3,4,5)
    val df = values.toDF("COL")

    ColSeq.foldLeft(df)((tempdf,x)=>{tempdf.withColumn(x,lit(0))}).show()

    val BazarDF = Seq(
      ("Veg", "tomato", 1.99),
      ("Veg", "potato", 0.45),
      ("Fruit", "apple", 0.99),
      ("Fruit", "pineapple", 2.59)
    ).toDF("Type", "Item", "Price")

    var BazarWithColumnDF = BazarDF.withColumn("Retailer",lit("null").as("StringType"))
      .withColumn("Quantity",lit(0.0).as("DoubleType"))

    BazarWithColumnDF.show()

    val ColNameWithDatatype = List(("Retailer", lit("null").as("StringType")),
      ("Quantity", lit(0.0).as("DoubleType")))

    var BazarWithColumnDFNew = ColNameWithDatatype.foldLeft(BazarDF)
    { (tempDF, colName) =>tempDF.withColumn(colName._1, colName._2)}

    BazarWithColumnDFNew.show()
    sc.stop()
    spark.stop()
  }

}
