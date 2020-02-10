import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,split}
object HelloWorld {

  def main(args: Array[String]): Unit = {
    println("hello")

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("HelloWorld").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
import spark.implicits._
    val RawRdd=sc.textFile("C:\\Users\\Owner\\Downloads\\sqoop_demo.txt").
      flatMap(x=>{x.split(" ")}).
      map(x=>{(x,1)}).reduceByKey((x,y)=>{(x+y)})
          RawRdd.foreach(x=>{println(x)})

      println("DataFrame")

    val RawDf=spark.read.format("csv").load("C:\\Users\\Owner\\Downloads\\sqoop_demo.txt").toDF("Text")
        .select(explode(split($"Text"," ")).as("Words")).groupBy($"Words").count()



    RawDf.show()

    sc.stop()
    spark.stop()
  }

}
