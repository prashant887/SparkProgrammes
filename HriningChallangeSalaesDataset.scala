import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType,StringType,FloatType,TimestampType,StructField,StructType,DateType}
import org.apache.spark.sql.functions.{sum,regexp_replace,rank,concat_ws,collect_list}
import org.apache.spark.sql.expressions.Window
object HriningChallangeSalaesDataset {

  case class ProductClass(id:Int,name:String,unit_price:Int)
  case class SalesClass(id:Int, product_id:Int, created_at:String, units:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("HriningChallangeSalaesDataset")
      .setMaster("local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    import spark.implicits._

    val ProductSchema=StructType(Seq(StructField("id",IntegerType,true),
      StructField("name",StringType,true),StructField("unit_price",IntegerType,true)
    ))

    val SalesSchema=StructType(Seq(StructField("id",IntegerType,true),StructField("product_id",IntegerType,true),
      StructField("created_at",DateType,true),StructField("units",IntegerType,true)
    ))

    val ProductDataSet=spark.read.
      schema(ProductSchema).
      option("mode","failfast")
     // .option("inferSchema","true")
      .option("header","true")
      .option("delimiter",",")
      .format("csv")
//      .load("D:\\pharmaceutical_sale_sample\\Product.csv")
      .load("D:\\pharmaceutical_sale_sample\\Product.csv")
      //.toDF()
      .as[ProductClass]
        .as("Products")


    //ProductDataSet.show()

    println("\n \n")

    val SalesDataSet=spark.read.
      schema(SalesSchema).
      option("mode","failfast")
     // .option("inferSchema","true")
      .option("header","true")
      .option("delimiter",",")
      .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
      .format("csv")
     // .load("D:\\pharmaceutical_sale_sample\\Sale.csv")
      .load("D:\\pharmaceutical_sale_sample\\Sale.csv")
      //.toDF()
      .as[SalesClass]
      .as("Sales")

    //SalesDataSet.show()


    val JoinedDataSet=ProductDataSet.join(SalesDataSet,$"Sales.product_id"===$"Products.id")
        .select(regexp_replace($"Sales.created_at","-",""),$"Sales.units"*$"Products.unit_price",$"Products.name")
      .withColumnRenamed("(Sales.units * Products.unit_price)","TotalPrice")
        .withColumnRenamed("regexp_replace(Sales.created_at, -, )","created_at")

    //JoinedDataSet.show()
    /*
    var JoinedDataSet=ProductDataSet.join(SalesDataSet,$"Sales.product_id"===$"Products.id")
    JoinedDataSet=JoinedDataSet.withColumn("TotalPrice",$"units"*$"unit_price")*/

    var GropedDataSet=JoinedDataSet.groupBy($"created_at",$"name")
        .agg(sum($"TotalPrice")).withColumnRenamed("sum(TotalPrice)","TotalPrice")
        .sort($"created_at",$"TotalPrice".desc)

    //GropedDataSet.show()

val windowExp=Window.partitionBy(GropedDataSet.col("created_at")).orderBy(GropedDataSet.col("TotalPrice").desc)
    GropedDataSet=GropedDataSet.withColumn("rank",rank over windowExp)

    val MostSelling=GropedDataSet.filter($"rank"===1)
        .select($"created_at",$"name")
      .sort($"created_at",$"name")
        .groupBy($"created_at")
        .agg(concat_ws(",",collect_list($"name")))
        .withColumnRenamed("created_at","day")
        .withColumnRenamed("concat_ws(,, collect_list(name))","top")
    MostSelling.show()

    sc.stop()
    spark.stop()

  }
}
