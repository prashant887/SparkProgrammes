import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,LongType}

object DiffInTwoFiles {

  case class Student(RollNo:Int,Name:String,Mobile:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("DiffInTwoFiles").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val schema=StructType(Seq(StructField("RollNo",IntegerType,false),
      StructField("Name",StringType,false),
      StructField("Mobile",LongType,false),
    ))
    val studentOne=spark.read.format("csv")
      .option("header","true")
      .schema(schema = schema)
      .load("/Users/pl465j/Downloads/student_one.csv").as("studentOne")
    studentOne.show(truncate = false)

    val studentTwo=spark.read.format("csv")
      .option("header","true")
      .schema(schema = schema)
      .load("/Users/pl465j/Downloads/student_two.csv").as("studentTwo")
    studentTwo.show(truncate = false)

    studentOne.join(studentTwo,$"studentOne.RollNo"===$"studentTwo.RollNo"
      and $"studentOne.Name"===$"studentTwo.Name"
      ,joinType = "anti").select($"studentOne.*").show(truncate = false)


  }
}
