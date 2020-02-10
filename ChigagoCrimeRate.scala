import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, explode, split,sum,to_timestamp,to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType,LongType,DoubleType}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Logger,Level}


object ChigagoCrimeRate {

  case class CrimeClass(CASE:String,DATE_OF_OCCURRENCE:String,BLOCK:String, IUCR:String,
  PRIMARY_DESCRIPTION:String, SECONDARY_DESCRIPTION:String, LOCATION_DESCRIPTION:String
  ,ARREST:String,DOMESTIC:String,BEAT:String,WARD:String,FBI_CD:String,
                   X_COORDINATE:Double,Y_COORDINATE:Double,LATITUDE:Double,LONGITUDE:Double,LOCATION:String
  )
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("CrimeRate").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val schema=StructType(
      Seq(
        StructField("CASE",StringType,true),
        StructField("DATE_OF_OCCURRENCE",StringType,true),
        StructField("BLOCK",StringType,true),
        StructField("IUCR",StringType,true),
        StructField("PRIMARY_DESCRIPTION",StringType,true),
        StructField("SECONDARY_DESCRIPTION",StringType,true),
        StructField("LOCATION_DESCRIPTION",StringType,true),
        StructField("ARREST",StringType,true),
        StructField("DOMESTIC",StringType,true),
        StructField("BEAT",StringType,true),
        StructField("WARD",StringType,true),
        StructField("FBI_CD",StringType,true),
        StructField("X_COORDINATE",DoubleType,true),
        StructField("Y_COORDINATE",DoubleType,true),
        StructField("LATITUDE",DoubleType,true),
        StructField("LONGITUDE",DoubleType,true),
        StructField("LOCATION",StringType,true)
      )
    )

    val CrimesDF=spark.read.format("csv")
      .schema(schema)
      .option("header","True")
        .option("inferSchema","False")
        .load("C:\\Users\\Owner\\Documents\\datasets\\crime.csv")
        .as[CrimeClass]
        .as("CrimeTbl")
CrimesDF.withColumn("DATE_OF_CRIME",to_timestamp($"DATE_OF_OCCURRENCE","mm/dd/yyyy hh:mm:ss a")).show(truncate = false)
CrimesDF.groupBy($"PRIMARY_DESCRIPTION").count()
    sc.stop()
    spark.stop()
  }
}
