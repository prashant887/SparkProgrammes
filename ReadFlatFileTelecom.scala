import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, sum, trim, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

case class Telco(	SeqNo:Int,
                 	customerID:String,
                 	gender:String,
                 	SeniorCitizen:Int,
                 	Partner:String,
                 	Dependents:String,
                 	tenure:Int,
                 	PhoneService:String,
                 	MultipleLines:String,
                 	InternetService:String,
                 	OnlineSecurity:String,
                 	OnlineBackup:String,
                 	DeviceProtection:String,
                 	TechSupport:String,
                 	StreamingTV:String,
                 	StreamingMovies:String,
                 	Contract:String,
                 	PaperlessBilling:String,
                 	PaymentMethod:String,
                 	MonthlyCharges:Double,
                 	TotalCharges:Double,
                 	Churn:String
                )

//def getCleanData(x:String):Telco={
//	var l=x.split(",")

//}
case class NonEmptyVal(Id:String,sex:String,cost:Double)
object ReadFlatFileTelecom {

	def default(x:String):NonEmptyVal={
		val splitVal=x.split(",")
		if (splitVal(20).trim.isEmpty ) {
			NonEmptyVal(splitVal(1),splitVal(2),0.0)
		}
		NonEmptyVal(splitVal(1),splitVal(2),x(20).toDouble)
	}

	def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("ReadFlatTelcome").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val telecomData=sc.textFile("/Users/pl465j/datasets/telecom_users.csv").
			filter(x=>{x.split(",")(0)!="SeqNo" }).
			map(x=>{default(x)}).
			map(x=>{(x.sex,x.cost)}).
			reduceByKey((x,y)=>{x+y}).
			collect()

		telecomData.foreach(x=>{println(x._1 +':'+x._2.toString )})

		var dataFrame=spark.read.format("csv").option("header","true").csv("/Users/pl465j/datasets/telecom_users.csv")
		dataFrame.filter($"TotalCharges"===" ").show()
		dataFrame=dataFrame.withColumn("TotalCharges",when(trim($"TotalCharges")==="",0).otherwise($"TotalCharges"))
		dataFrame.na.fill(0,Array("TotalCharges"))
		dataFrame.filter($"TotalCharges"===0).show()
		dataFrame.groupBy($"gender").agg(sum($"MonthlyCharges").as("avgSum"),count($"MonthlyCharges").as("Count")).show()

		sc.stop()
		spark.stop()

	}

}
