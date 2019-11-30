import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.max
object WindowFunctions {

  case class Salary(dep:String,emp:Long,salary:Long)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("WindowFunctions").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
import spark.implicits._
    val empSal=Seq(Salary("HR",1,5000),
      Salary("IT",2,6000),Salary("Sales",3,7000),
      Salary("IT",4,9000),Salary("Sales",5,8000),Salary("HR",9,10000)
    )

    val SalDS=empSal.toDS()

    val salDesc=Window.partitionBy(SalDS.col("dep"))//.orderBy(SalDS.col("salary"))

    val SalDiff=max(SalDS.col("salary")).over(salDesc)

   // SalDS.select(SalDiff).show()

    SalDS.select($"dep",$"salary",SalDiff).show()

    SalDS.select("dep","salary").show()
    SalDS.filter("salary>5000").show()
    SalDS.filter($"salary">7000).show()

    val salDF=empSal.toDS()
    salDF.filter($"salary">9000).show()
    salDF.filter(x=>{x.salary>6000}).show()
    sc.stop()
    spark.stop()
  }
}
