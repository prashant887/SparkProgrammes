import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SparkSession,Column}
import org.apache.spark.sql.functions.{split,explode,lit}

object DFFoldLeft {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("DFFoldLeft")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val BazarDF = Seq(
      ("Veg", "tomato", 1.99),
      ("Veg", "potato", 0.45),
      ("Fruit", "apple", 0.99),
      ("Fruit", "pineapple", 2.59)
    ).toDF("Type", "Item", "Price")

    BazarDF.show()

    //val ColNameWithDatatype = List[String]("A","B","C")
    var BazarWithColumnDF = BazarDF.withColumn("Retailer",lit("null").as("StringType"))
      .withColumn("Quantity",lit(0.0).as("DoubleType"))

   val ColNameWithDatatype = List(("Retailer", lit("null").as("StringType")),
      ("Quantity", lit(0.0).as("DoubleType")))
    println(ColNameWithDatatype)

    //val newDF=ColNameWithDatatype.foldLeft(BazarDF)((tempdf,col)=>{tempdf.withColumn(col,lit(0).as("StringType"))})

    var BazarWithColumnDF1 = ColNameWithDatatype.foldLeft(BazarDF)
    { (tempDF, colName) =>tempDF.withColumn(colName._1, colName._2)}
    BazarWithColumnDF1.show()
  }
}
