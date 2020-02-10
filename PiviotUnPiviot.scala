import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array,when,avg}

object PiviotUnPiviot {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("PiviotUnpiviot")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    val df = data.toDF("Product","Amount","Country")
    df.show()

    df.groupBy($"Product").agg(avg($"Amount").alias("AvgSal")).show()
    df.groupBy("Product").pivot($"Country").avg("Amount").show()
    df.groupBy("Country").pivot($"Product").sum("Amount").show()

    df.groupBy("Product","Country")
      .sum("Amount").as("TotalAmt") .groupBy("Product").sum("sum(Amount)").show()


  }
}
