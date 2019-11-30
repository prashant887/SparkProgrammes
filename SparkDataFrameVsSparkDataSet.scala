import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkDataFrameVsSparkDataSet {

  case class Student(name:String,age:Int,dept:String,status:String,remark:String)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("SparkDataFrameVs").setMaster("local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    val data=Seq(Student("Suresh",12,"SC","pass","improve"),
      Student("Ramesh",17,"MA","pass","Nice"),
      Student("Ganesh",18,"BA","fail","Hard Study"),
      Student("Vignesh",14,"MS","justpass","Lot Of Improvement")
    )

    val rdd=sc.makeRDD(data)

    import spark.implicits._
    val dataFrame=rdd.toDF()

    dataFrame.printSchema()

    val dataSet=rdd.toDS()

    dataSet.printSchema()

    val dsResult=dataSet.filter(x=>{x.age>15})

    dsResult.show()

    // dfResult=dataFrame.filter(x=>{x.age>15}) does not work
    val dfResult=dataFrame.filter(x=>{x.getAs[Int]("age")<15})

    dfResult.show()

    dataFrame.select("name").show()

    dataSet.select("name").show()

    //Rdd created from DataFrame cant preserve schema

    val rddFromDF=dataFrame.rdd

    val rddFromDFOps=rddFromDF.map(x=>{x(0)}) /*map(x=>{x.name}) not accessble*/

    rddFromDFOps.foreach(x=>{println(x)})

    //Rdd created from Dataset will preserve schema
    val rddFromDS=dataSet.rdd

    val rddFromDSOps=rddFromDS.map(x=>{x.name})

    rddFromDSOps.foreach(x=>{println(x)})
    sc.stop()
    spark.stop()
  }
}
