import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{spark_partition_id,sum,avg}

object googlePlayStoreData {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("googlePlayStoreData").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val playStoreData=
      spark.read.format("csv").
      option("header","true").
      load("/Users/pl465j/Downloads/dataset-master/googleplaystore.csv")
    playStoreData.show(truncate = false)
    playStoreData.groupBy($"Category").count().show()
    playStoreData.repartition($"Category")
      .withColumn("parationIds",spark_partition_id())
      .groupBy($"parationIds").count()
      .show(truncate = false)
    sc.stop()
    spark.stop()
    spark.close()
  }
}
