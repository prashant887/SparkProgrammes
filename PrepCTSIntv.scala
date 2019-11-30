import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField,StructType,IntegerType,StringType}
import org.apache.spark.sql.functions.{max,rank,sum,concat_ws,collect_list,avg}
import org.apache.spark.sql.expressions.Window

object PrepCTSIntv {

  case class CitySal (Id:Int,Name:String,City:String,Cntry:String,Sal:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("CtsInterview").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val rawRdd=sc.textFile("D:\\citysal.csv")
    val keyvalRdd=rawRdd.map(x=>{x.split("\\s+")}).map(x=>{(x(2),x(4).toInt)})
      .reduceByKey((x,y)=>{ if (x>y) {x} else {y}})

    keyvalRdd.foreach(x=>{println(x)})

    val SalCityType=StructType(Seq(StructField("Id",IntegerType,true),
      StructField("Name",StringType,true),StructField("City",StringType,true),StructField("Cntry",StringType,true),
      StructField("Sal",IntegerType,true)
    ))

    val CitySalDataFrame=spark.read.schema(SalCityType)
        .option("header","false")
        .option("delimiter"," ")
        .format("csv")
        .load("D:\\citysal.csv")
        .as[CitySal]
        .as("CitySal")
    //CitySalDataFrame.show()

    CitySalDataFrame.printSchema()
    CitySalDataFrame.createOrReplaceGlobalTempView("CitySalDataTable")
    spark.sql("select City,Sal,rank() over(Partition by City order by Sal desc) as rank   from global_temp.CitySalDataTable").show()

  // val WindowExp=Window.partitionBy(CitySalDataFrame("$City")).orderBy(CitySalDataFrame("$Sal"))
  val WindowExp=Window.partitionBy(CitySalDataFrame.col("City")).orderBy(CitySalDataFrame.col("Sal").desc)
    val RankedOP=CitySalDataFrame.withColumn("rank",rank over WindowExp)
    RankedOP.filter("rank==2").show()
    RankedOP.groupBy($"City").agg(sum($"Sal").as("Sum")).show()
    RankedOP.groupBy($"City").agg(avg($"Sal").alias("Average")).show()

    sc.stop()
    spark.stop()
  }
}
