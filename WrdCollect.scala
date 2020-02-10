import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}

object WrdCollect {

  val spark = SparkSession.builder()

    .master("local[*]")

    .appName("SparkExercise")

    .getOrCreate()

  val sc=spark.sparkContext

  val data=sc.textFile("abc").flatMap(x=>{x.split(" ")})
  .flatMap(x=>{x.split(",")})
    .flatMap(x=>{x.split(".")})
    .filter(x=>{x.toLowerCase.contains("good")})





}
