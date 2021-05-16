import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{sum,from_json,json_tuple,spark_partition_id}

object trasposeDataFrame {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("trasposeDataFrame").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val marksData = spark.read.option("header", "true")
      .option("inferschema","true")
      .csv("/Users/pl465j/Downloads/marks.csv").as("marksData")
    marksData.printSchema()
    marksData.show(truncate = false)
    println(marksData.rdd.getNumPartitions)
//to povit there shd be one numberic col
    val povitDf=marksData.groupBy("ROLL_NO").pivot("SUBJECT").max("MARKS").as("Subjects")
    povitDf.printSchema()
    povitDf.show(truncate = false)
    marksData.groupBy("ROLL_NO").pivot("SUBJECT").sum("MARKS").show()
    val totalDF=marksData.groupBy("ROLL_NO").agg(sum("MARKS").as("TotalMarks")).as("total")
    totalDF.printSchema()
    totalDF.show(truncate = false)
    povitDf.join(totalDF,$"Subjects.ROLL_NO"===$"total.ROLL_NO").select($"Subjects.*",$"total.TotalMarks").show()
    println(povitDf.rdd.getNumPartitions)
    println(povitDf.isEmpty)
    povitDf.withColumn("partationId",spark_partition_id()).groupBy($"partationId").count().show()

    spark.stop()
    spark.close()
  }

}
