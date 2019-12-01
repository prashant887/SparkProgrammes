import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField,StringType,IntegerType,DateType,StructType}
import org.apache.spark.sql.functions.{lower}
object AttandenceExecrcise {

  case class StudentClass(Name:String,ID:Int,DOB:String)
  case class AttendenceClass(Date:String,Id:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val confg=new SparkConf().setAppName("Attandence").setMaster("local[*]")
    val spark=SparkSession.builder().config(confg).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val studentSchema=StructType(Seq(StructField("Name",StringType,false),
      StructField("ID",IntegerType,false),StructField("DOB",StringType,false)))

    val attendenceSchema=StructType(Seq(StructField("Date",StringType,true),
      StructField("Id",IntegerType,true)))

   val StudentDF=spark.read
        .schema(studentSchema)
      .format("csv")
      .option("header","true")
        .option("dateFormat","dd-mmm-yyyy")
      .option("delimiter",",")
      .load("D:\\Students.csv")
        .as[StudentClass]
        .as("Student")

    //StudentDF.show()

    val AttendenceDF=spark.read
        .schema(attendenceSchema)
        .format("csv")
        .option("header","true")
        .option("delimiter",",")
      .option("dateFormat","Mmm-dd-yyyy")
        .load("D:\\attendenc.csv")
        .as[AttendenceClass]
        .as("Attendance")

    //AttendenceDF.show()

    val SitaDF=StudentDF.filter(lower($"Name")==="sita")
        .join(AttendenceDF,$"Attendance.Id"===$"Student.ID")
        .select($"Student.Name",$"Attendance.Date")
        .as("Sita")

    //SitaDF.show()
    val NonSitaDF=AttendenceDF.join(SitaDF,$"Attendance.Date"===$"Sita.Date")
      .join(StudentDF,$"Attendance.Id"===$"Student.ID")
        .filter(lower($"Student.Name")=!="sita")
        .select($"Student.Name",$"Attendance.Date").as("NoSita")

    println("\n Students who Come when Sita Comes \n")

    NonSitaDF.show()

    val SitaNotCome=AttendenceDF.join(SitaDF,$"Attendance.Date"===$"Sita.Date","leftanti")
      .select($"Attendance.Date")
        .as("SitaNot")
    //SitaNotCome.show()

    val AttandeceDiscard=AttendenceDF.join(SitaNotCome,$"Attendance.Date"===$"SitaNot.Date")
      .join(StudentDF,$"Attendance.Id"===$"Student.ID")
      .select($"Attendance.Date",$"Attendance.Id",$"Student.Name")
      .as("Discard")

    //AttandeceDiscard.show()

    val FinalDataSet=AttendenceDF
      .join(AttandeceDiscard,$"Attendance.Id"===$"Discard.Id","leftanti")
      .join(StudentDF,$"Attendance.Id"===$"Student.Id")
      .filter(lower($"Student.Name")=!="sita")
      .select($"Student.Name" ,$"Attendance.Date")
        .as("FinalData")

    println("\n Students Who come only when Sita Comes \n")
    FinalDataSet.show()


    sc.stop()
    spark.stop()
  }
}
