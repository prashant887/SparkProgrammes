import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType,TimestampType}
import org.apache.spark.sql.Row
object ChiagoCrimeCreateDF {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("ChiagoCrimeDf").setMaster("local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    val crimeRDD=sc.textFile("D:\\Crimes_-_2019.csv")

    val dateTypeRdd=crimeRDD
      .map(x=>{x.split(",")})
        .filter(x=>{x(0).matches("\\d+")})
        .map(x=>{val mmddyyyy=x(2).substring(0,10).split("/")
        val yyyymm=mmddyyyy(2).concat(mmddyyyy(0)).toInt
          Row(yyyymm,x(5))})

    dateTypeRdd.foreach(x=>{println(x)})

    val schema=StructType(Seq(StructField("Date",IntegerType,true),StructField("Type",StringType,true)))

    val DataFrame=spark.createDataFrame(dateTypeRdd,schema)

DataFrame.show()
    sc.stop()
    spark.stop()
  }

}
