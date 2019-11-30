import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{when,lit,sum}

object BankTransactionDataFrame {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val config=new SparkConf().setAppName("BankTransactionDataFrame").setMaster("local[*]")

    val spark=SparkSession.builder().config(config).getOrCreate()

    val Schema=StructType(List(StructField("trans_id",IntegerType,false),
      StructField("account_id",IntegerType,false),
      StructField("date",TimestampType,false),
      StructField("type",StringType,false),
      StructField("operation",StringType,false),
      StructField("amount",FloatType,false),
      StructField("balance",FloatType,false),
      StructField("k_symbol",StringType,true),
      StructField("bank",StringType,true),
      StructField("account",StringType,true)
    ))

    val BankDataFrame=spark
        .read.format("csv")
        .option("delimiter",";")
        .option("header","true")
        .option("nullValue","NA")
        .option("timestampFormat","YYMMDD")
        .option("mode","failfast")
        .option("path","C:\\Users\\PLaxmikant\\Downloads\\data_berka\\trans.asc")
      .schema(Schema)
        .load()

    /*case class BankSchema(age: Int, job: String, marital:String, education:String, default:String, balance:Int, housing:String, loan:String, contact:String, day:Int, month:String, duration:Int, campaign:Int, pdays:Int, previous:Int, poutcome:String, y:String)
val bankData = sc.textFile("/user/myuser/Project_Bank.csv").map(_.split(";")).map(p => BankSchema(p(0).toInt, p(1), p(2),p(3),p(4), p(5).toInt, p(6), p(7), p(8), p(9).toInt, p(10), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toInt, p(15), p(16))).toDF()

     */
    import spark.implicits._

    BankDataFrame.printSchema

    BankDataFrame.show()

    /* lit(-1)*/
    val ModifiedData=BankDataFrame.select($"account_id",$"type",
      (when ($"type"==="VYDAJ",$"amount" * -1 ).otherwise($"amount")).alias("amount")
    )
      .groupBy($"account_id")
        .agg(sum($"amount").alias("Balance")).sort($"account_id")

    ModifiedData.filter("account_id==1").show(50)

ModifiedData.show(50)

    val CreditDebitCount=BankDataFrame.select($"account_id",
      (when ($"type"==="VYDAJ",1).otherwise(0) ).alias("VYDAJ"),
      (when ($"type"==="PRIJEM",1).otherwise(0).alias("PRIJEM"))
    ).groupBy("account_id")
        .agg(sum($"VYDAJ").alias("DEBIT"),sum($"PRIJEM").alias("CREDIT"))

    CreditDebitCount.show(50)
    spark.close()
  }
}
