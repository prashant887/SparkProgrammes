import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, from_unixtime, lit, to_date, unix_timestamp, when}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType, TimestampType}

object checkForMultipleNullCols {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("checkForMultipleNullCols").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val schema=StructType(Seq(StructField("DateTime",TimestampType,false),StructField("Temperature",FloatType,false),
      StructField("Humidity",FloatType,false),StructField("WindSpeed",FloatType,false),
      StructField("Pressure",FloatType,false),StructField("Summary",StringType,false)))

    val dataDf=spark.read.format("csv").option("header","true").option("timestampFormat","M/d/y h:m a")
     // .schema(schema = schema)
      .load("/Users/pl465j/Downloads/missingVals.csv")

  var empDf=spark.emptyDataFrame.withColumn("Id",lit("")).withColumn("columns",lit(" "))
    dataDf.columns.foldLeft(dataDf)((df,cols)=>{
      df.withColumn(cols+"Null",when(col(cols).isNull ,"Y").otherwise("N"))
    }).show()

    dataDf.columns.foldLeft(dataDf)((df,cols)=> {
    empDf=empDf.unionByName(df.withColumn("columns",lit(cols)).select($"Id",$"columns").filter(col(cols).isNull))
      dataDf
    }
    )
    empDf.show()
    empDf.groupBy($"columns").agg(concat_ws(",",collect_list($"Id")).as("IdList")).show()
    empDf.groupBy($"Id").agg(concat_ws(",",collect_list($"columns")).as("ColList")).show()
    empDf.groupBy($"columns").pivot($"Id").count().show()
    empDf.groupBy($"Id").pivot($"columns").count().show()

    spark.stop()
    spark.close()
  }

}
