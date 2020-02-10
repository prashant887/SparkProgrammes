import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import  org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField,StringType,StructType,IntegerType,FloatType,DateType}
import org.apache.spark.sql.functions._
object EmployeeMgr {
  case class Employee( emp_id:Int,emp_name:String, job_name:String   ,manager_id:Int , hire_date:String
                       , salary:Float  ,commission:Double , dep_id:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("EmployeeMgr")
    val spark=SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val schema=StructType(Seq(StructField("emp_id",IntegerType,false),
      StructField("emp_name",StringType,false),StructField("job_name",StringType,false),
      StructField("manager_id",IntegerType,false),StructField("hire_date",DateType,false),
      StructField("salary",FloatType,false),StructField("commission",FloatType,false),StructField("dep_id",IntegerType,false)
    ))
    val EmployeeDS=spark.read.format("csv").schema(schema)
        .option("header","true")
        .option("delimiter",",")
        .option("dateFormat","yyyy-mm-dd")
        .option("nullValue","NA")
        .load("D:\\empmgr.csv")
        .as[Employee]
        .as("Employee")
    //EmployeeDS.show()
    EmployeeDS.as("E1").join(EmployeeDS.as("E2"),$"E1.emp_id"===$"E2.manager_id")
        .select($"E1.*").show()
    EmployeeDS.groupBy($"dep_id").agg(sum($"salary")).show()

    val EmpRdd=sc.textFile("D:\\empmgr.csv")
        .map(x=>{x.split(",")})
        .filter(x=>{x(0).matches("\\d+")})
        .map(x=>{(x(7).toInt,x(5).toFloat)})
        .reduceByKey((x,y)=>{x+y})

    EmpRdd.foreach(x=>{println(x)})

    sc.stop()
    spark.stop()

  }
}
