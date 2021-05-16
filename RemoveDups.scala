import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank,row_number}
object RemoveDups {

  case class ph(Brand:String,Type:String,Price:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val data=Seq(("Apple","Iphone-12","1000"),
      ("Apple","Iphone-12","1000"),
      ("Apple","Iphone-12","2000"),
      ("Apple","Iphone-11","3000"))
    val cols=Seq(("Brand","Type","Money"))
    val schema=StructType(Seq(StructField("Band",StringType),StructField("Type",StringType)
      ,StructField("Price",StringType)))

    val conf=new SparkConf().setAppName("RemoveDup").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val dataFrame=data.toDF("Brand","Type","Price")
    val dataSets=dataFrame.as[ph]
    dataSets.show()
    dataFrame.show()
    dataFrame.dropDuplicates().show()
    dataFrame.dropDuplicates("Type").show()
    dataFrame.dropDuplicates("Type","Brand").show()

    val sc=spark.sparkContext

    val rdd=sc.parallelize(data).map(x=>{Row(x._1,x._2,x._3)})
    val newDF=spark.createDataFrame(rdd,schema)
    newDF.show()
    newDF.groupBy($"Band",$"Price",$"Type").count().where($"count">1).show()

    val win=Window.partitionBy($"Band",$"Type").orderBy($"Price".desc)
    newDF.withColumn("RowNum",row_number().over(win)).show()

    sc.stop()

    spark.close()
  }

}
