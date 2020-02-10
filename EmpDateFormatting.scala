import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, when,datediff,date_format,count,sum,min,max,row_number,rank,dense_rank}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, DateType}
import org.apache.spark.sql.expressions.Window

object EmpDateFormatting {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("EmpDateFormat")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val EmpSchema=StructType(Seq(
      StructField("emp_id",IntegerType,false),
      StructField("emp_name",StringType,false),
        StructField("job_name",StringType,false),
      StructField("manager_id",IntegerType,false),
      StructField("allocated_date",DateType,false),
      StructField("salary",FloatType,false),
      StructField("commission",FloatType,true),
      StructField("dep_id",IntegerType,false),
      StructField("hire_date",DateType,false)
    ))

    val DeptSchema=StructType(Seq(StructField("dep_id",IntegerType,false),
     StructField ("dep_name",StringType,false),StructField("dep_location",StringType,false)
    ))

    var EmpDF=spark.read.format("csv").schema(EmpSchema)
      .option("header","True")
      .option("DateFormat","yyyy-MM-dd")
      .load("C:\\Users\\Owner\\Documents\\datasets\\emp.csv").as("Emp")

    var DeptDF=spark.read.format("csv").schema(DeptSchema)
      .option("header","True")
      .load("C:\\Users\\Owner\\Documents\\datasets\\dept.csv").as("Dept")

    EmpDF.printSchema()
    EmpDF.withColumn("NumDays",datediff($"allocated_date",$"hire_date"))
      .withColumn("StringHireDate",date_format($"hire_date","dd-MMM-yyyy"))
        .filter($"NumDays" > 30)

    DeptDF.show()

    EmpDF.join(DeptDF,$"Emp.dep_id"===$"Dept.dep_id")
      .select($"Emp.salary",$"Dept.dep_name").groupBy($"Dept.dep_name").sum("Emp.salary")
      .withColumnRenamed("sum(salary)","TotalSumSal")
      .show()


    EmpDF.join(DeptDF,$"Emp.dep_id"===$"Dept.dep_id")
      .select($"Emp.salary",$"Dept.dep_name").groupBy($"Dept.dep_name")
      .agg(count("*").alias("Count"),sum($"Emp.salary").as("TotalSal"),
        avg($"Emp.salary").as("AvgSal"),min($"Emp.salary").as("MinSal"),max($"Emp.salary").as("MaxSal")
    ).coalesce(1).write.mode("append").format("parquet").save("C:\\Users\\Owner\\Documents\\datasets\\out")
      //.parquet("C:\\Users\\Owner\\Documents\\datasets\\out")

    val window=Window.orderBy($"Emp.salary")
    EmpDF.join(DeptDF,$"Emp.dep_id"===$"Dept.dep_id")
      .select($"Emp.salary",$"Dept.dep_name")
      .withColumn("RowNum",row_number().over(window))
      .withColumn("Rank" , rank().over(window))
      .withColumn("DenseRank",dense_rank().over(window)).show()
  }


}
