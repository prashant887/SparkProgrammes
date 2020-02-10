import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SparkSession,Column}
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType,DateType}

object RePartionDataFrame {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("RepartionDataFrame")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val schema=StructType(Seq(StructField("Name",StringType,false),
      StructField("Age",IntegerType,true),StructField("Country",StringType,false),
      StructField("Year",IntegerType,true),StructField("Date",DateType,true),
      StructField("Event",StringType,true),StructField("Gold",IntegerType,true)
      ,StructField("Silver",IntegerType,true),StructField("Bronze",IntegerType,true),
      StructField("Total",IntegerType,true)))
    //val olympicData=sc.textFile("D:\\olympix_data.csv",3)
    val olympicData=spark.read.format("csv")//.schema(schema)
      //  .option("header","False")
      //  .option("DateFormat","dd-MM-yyyy")
        .load("D:\\olympix_data.csv").toDF("Name","Age","Country","Year","Date","Event","Gold","Silver","Bronze","Total")
    olympicData.show(truncate = false)
    println(olympicData.rdd.partitions.size)
    val repatineolympicData=olympicData.repartition(3,$"Country")
    println(repatineolympicData.rdd.partitions.size)


    sc.stop()
    spark.stop()
  }
}
