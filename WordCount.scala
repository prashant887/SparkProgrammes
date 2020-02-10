import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, explode, split,sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Logger,Level}
object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()

      .master("local[*]")

      .appName("SparkExercise")

      .getOrCreate()

    val inputDataPath = "./src/main/resources/commercesparis.csv"
    import spark.implicits._
    val schema=StructType(Seq(StructField("Col1",IntegerType,true),StructField("Col2",StringType,true),StructField("IRIS",StringType,true),
      StructField("ILOT",StringType,true)
    ))
    val dataFrame=spark.read.format("csv").schema(schema).option("header","false").load(inputDataPath)

    val windowFu=Window.partitionBy(dataFrame("col1")).orderBy("col2")
    dataFrame.withColumn("CODE_GLOBAL",concat($"IRIS ",$"ILOT"))
    dataFrame.withColumn("rank",$"col1".over(windowFu) ).filter($"rank"<=5)

    dataFrame.groupBy($"col1",$"col2").count()
  }

  def commerecePerituationTotal(comm : DataFrame):DataFrame={
    val returndataframe=comm.groupBy("co1","col2").agg(sum("col3"))

    return returndataframe
  }
}
