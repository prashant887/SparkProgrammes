import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{struct,col,lower}

object StructTypeStructField {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val confg=new SparkConf().setMaster("local[*]").setAppName("StructTypeStructField")

    val spark=SparkSession.builder().config(confg).getOrCreate()

    val sc=spark.sparkContext

    val data=Seq(Row("Steel",9000,"yes"),
      Row("IT",100000,"yes"),
      Row("TexTile",9000,"no"),
      Row("Automobile",8000,"Yes"))

    val schema=StructType(List(
      StructField("industry",StringType,true),
      StructField("emp_size",IntegerType,true),
      StructField("outside_country",StringType,true)
    ))

    val df1=spark.createDataFrame(sc.parallelize(data),schema)
df1.printSchema()
    df1.show()

    //adding new struct
val df2=df1.withColumn("level",struct((col("emp_size")>8000).as("big_org"),
  lower(col("outside_country")==="yes").as("MNC")
))

    println("\n DF2 \n")
    df2.printSchema()
    df2.show()

    println("\n Nested Fields \n")

    df2.select(col("industry"),
      col("level")("big_org").as("big_org")  ,
      col("level")("MNC").as("MNC") ).show()


    sc.stop()
    spark.stop()
  }
}
