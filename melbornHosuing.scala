import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, from_unixtime, lit, to_date, unix_timestamp, when,avg,count,sum,max,min}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType, DateType,IntegerType,LongType}

object melbornHosuing {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("melbornHosuing").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    //spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val schema=StructType(Seq(StructField("Suburb",StringType,false),StructField("Address",StringType,false),
      StructField("Rooms",IntegerType,false),StructField("Type",StringType,false),
      StructField("Price",FloatType,false),StructField("Method",StringType,false),
      StructField("SellerG",StringType,false),StructField("Date",DateType,false),
      StructField("Distance",FloatType,false),StructField("Postcode",FloatType,false),
      StructField("Bedroom2",FloatType,false),StructField("Bathroom",FloatType,false),
      StructField("Car",FloatType,false),StructField("Landsize",FloatType,false),
      StructField("BuildingArea",FloatType,false),StructField("YearBuilt",FloatType,false),
      StructField("CouncilArea",StringType,false),StructField("Lattitude",FloatType,false),
      StructField("Longtitude",StringType,false),StructField("Regionname",StringType,false),
      StructField("Propertycount",FloatType,false)))

    val melbornData=spark.read.format("csv").option("header","true").option("dateFormat","d/M/y")
    .schema(schema = schema)
      .load("/Users/pl465j/Downloads/melb_data.csv")
      .withColumn("Postcode",$"Postcode".cast(IntegerType))
      .withColumn("Bedroom",$"Bedroom2".cast(IntegerType))
      .withColumn("Bathroom",$"Bathroom".cast(IntegerType))
      .withColumn("Landsize",$"Landsize".cast(IntegerType))
      .withColumn("BuildingArea",$"BuildingArea".cast(IntegerType))
      .withColumn("YearBuilt",$"YearBuilt".cast(IntegerType))
      .withColumn("Propertycount",$"Propertycount".cast(IntegerType))
      .withColumn("Price",$"Price".cast(IntegerType))

    melbornData.na.drop().groupBy($"Suburb",$"Bedroom")
      .agg(avg($"Price").as("AvgPrice"),max($"Price").as("MaxPrice"),min($"Price").as("MinPrice"),count($"Price").as("NumHouses"))
      .sort($"Suburb",$"Bedroom")
      .show(truncate = false)

    //melbornData.show(truncate = false)

    spark.stop()
    spark.close()
  }
}
