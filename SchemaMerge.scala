import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
object SchemaMerge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("SchemaMerge")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.mode(SaveMode.Overwrite).parquet("D:\\test_table\\key=1")

    val cubesDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.mode(SaveMode.Overwrite).parquet("D:\\test_table\\key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("D:\\test_table")
    mergedDF.printSchema()
    mergedDF.sort($"value",$"key").show()

    sc.stop()
    spark.stop()

  }
}
