import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round,avg}

import scala.util.Random._
object NathielOsgood {

  case class Person(name:String,age:Int,income:Double)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("NathielOsgood")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    val p1=Person("Prashant",32,100.20)

    val Person(name,age,income)=p1

    println("Name : %s age :%d income : %f".format(name,age,income))

    val population=Array.fill(1000)(Person(nextString(3).mkString(""),nextInt(50),nextDouble()*1000))
      //Array.tabulate(3,4)
//nextString generate random string,nextInt genreate string , nextdouble generate double

    val populationRDD=sc.parallelize(population)

    populationRDD.filter(x=>{x.age<25}).foreach(x=>{println(x)})

    println(populationRDD.getStorageLevel )
    println(populationRDD.partitioner)
    println(populationRDD.partitions.mkString(","))

    import  spark.implicits._
    val populationDF=populationRDD.toDF()
    val populationDF2=spark.createDataFrame(populationRDD)
    val populationDS=populationDF2.as[Person]

    populationDS.
      filter(x=>{x.age>20})
      .groupBy(round($"age"/5)*5)
      .agg(avg($"income"))
      .withColumnRenamed("(round((age / 5), 0) * 5)","AgeCategeory")
      .withColumnRenamed("avg(income)","AvgIncome")
      .orderBy($"AgeCategeory")
      .show()

    populationDS.select(round($"income"/1000.0)*1000.0,($"age"/10)*10)
      .withColumnRenamed("(round((income / 1000.0), 0) * 1000.0)","IncomeCategeory")
      .withColumnRenamed("((age / 10) * 10)","AgeCategeory")
      .stat
      .crosstab("AgeCategeory","IncomeCategeory")
        .show()

    sc.stop()
    spark.stop()
  }
}
