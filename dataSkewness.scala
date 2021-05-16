import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{spark_partition_id}
object dataSkewness {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("dataSkewness").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val bankData=spark.read.format("csv").
      option("header","true").
      load("/Users/pl465j/Downloads/dataset-master/BankChurners.csv").repartition($"Card_Category")
    bankData.withColumn("parationIds",spark_partition_id()).groupBy($"parationIds").count().show()

    spark.stop()
    spark.close()
  }

}
