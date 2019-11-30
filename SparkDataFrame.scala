import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
import  org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
object SparkDataFrame {

  def parseGender(g: String):String = {
    if (g.startsWith("m")||g.startsWith("M")){
      return "male"
    }
    else if(g.startsWith("f")||g.startsWith("F")) {
      return "female"
    }
    else
      {
        return "Transgender"
      }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config=new SparkConf()
      .setAppName("SparkDataFrame")
      .setMaster("local[*]")

    val sc=new SparkContext(config)

    val spark=SparkSession
      .builder()
      .config(config)
      .getOrCreate()

val df=spark.read
  .format("csv")
  .option("header","true")
  .option("nullValue","NA")
  .option("timestampFormat","yyyy-MM-dd'T'HH:mm​:ss")
  .option("mode","failfast")
  .option("path","D:\\mental-health-in-tech-survey\\survey.csv")
  .load()

    import spark.implicits._

    println("\n Number of Partations %d \n".format(df.rdd.getNumPartitions))

    df.printSchema

    val FileterdData=df.select("Gender","Country").filter("Age>30")
    FileterdData.foreach(x=>{println(x)})

    FileterdData.show


    println("\n Mental Health Using SC \n")
    val MentalRawData=sc.textFile("D:\\mental-health-in-tech-survey\\survey.csv")

    val TreatMentData=MentalRawData
      .map(x=>{x.split(",")})
      .filter(x=>{x(7).replaceAll("\"","").equals("Yes")||x(7).replaceAll("\"","").equals("No")})
      .map(x=>{(parseGender(x(2).replaceAll("\"","")),x(7).replaceAll("\"",""))})
     .map(x=>{(x,1)})
        .reduceByKey((x,y)=>{x+y})
        .sortBy(x=>{x._1._1})
        .collect()

    TreatMentData.foreach(x=>{println(x._1._1,x._1._2,x._2)})
//TreatMentData.foreach(x=>{println(x)})

    val parseGenderUDF=udf(parseGender _)
    val treatMentDataDf=df.select(parseGenderUDF($"Gender").alias("Gender"),
      (when($"treatment"==="Yes",1).otherwise(0)).alias("All-Yes"),
      (when($"treatment"==="No",1).otherwise(0)).alias("All-No")

    ).
      groupBy($"Gender")
      .agg(sum($"All-Yes"),sum($"All-No"))
    println("\n Mental Health Using DF \n")


    treatMentDataDf.foreach(x=>{println(x)})

    println("\n Show Schema \n")

    println(df.schema)

      }

}
