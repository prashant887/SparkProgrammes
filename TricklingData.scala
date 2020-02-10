import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SparkSession,Column}
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType,DateType,FloatType}
import org.apache.spark.sql.functions.{when}

object TricklingData {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("TricklingData")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val DataSchema=StructType(
      Seq(
        StructField("Id",IntegerType,false),
        StructField("chain",IntegerType,false),
        StructField("dept",IntegerType,false),
        StructField("category",IntegerType,false),
        StructField("company",IntegerType,false),
        StructField("brand",IntegerType,false),
        StructField("transaction_date",DateType,false),
        StructField("productsize",IntegerType,false),
        StructField("productmeasure",StringType,false),
        StructField("purchasequantity",IntegerType,false),
        StructField("purchaseamount",FloatType,false),
        StructField("file_date",DateType,false)
      )
    )
val TransactionData=spark.read.format("csv").schema(DataSchema)
  .option("header","true")
  .option("DateFormat","dd-MM-yy")
  .load("D:\\transactions-sample.csv")

    TransactionData.withColumn("DateMismatch",
      when($"transaction_date" === $"file_date","N").otherwise("Y"))
        .withColumn("ValidTransaction",when($"transaction_date">$"file_date","N").otherwise("Y"))
      .filter($"ValidTransaction"==="N")
        .show()

    //TransactionData.show(truncate = false)
  }
}
