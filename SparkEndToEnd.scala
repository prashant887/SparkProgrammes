import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date,from_unixtime,unix_timestamp}
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType,TimestampType}
object SparkEndToEnd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SparkEndToEnd").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val schema=StructType(Seq(StructField("DateTime",TimestampType,false),StructField("Temperature",FloatType,false),
      StructField("Humidity",FloatType,false),StructField("WindSpeed",FloatType,false),
      StructField("Pressure",FloatType,false),StructField("Summary",StringType,false)))

    val dataDf=spark.read.format("csv").option("header","true").option("timestampFormat","M/d/y h:m a")
      .schema(schema = schema).load("/Users/pl465j/Downloads/dataset-master/weatherHistory.csv")

    dataDf.show()

    dataDf.filter($"Temperature".isNull or $"Humidity".isNull or $"WindSpeed".isNull or $"Pressure".isNull ).show(truncate = false)
    dataDf.filter($"Temperature".isNull and $"Humidity".isNull and $"WindSpeed".isNull and $"Pressure".isNull ).show(truncate = false)
    val cleanedDf=dataDf.filter($"Temperature".isNotNull and $"Humidity".isNotNull and $"WindSpeed".isNotNull and $"Pressure".isNotNull )
    /*
    cleanedDf.withColumn("DateOnly",to_date(
      from_unixtime(
        unix_timestamp($"DateTime","MM/d/yy hh:mm a")
      ).cast(TimestampType)
    )
    ).show()
*/
    cleanedDf.show()

    spark.stop()
    spark.close()



  }

}
