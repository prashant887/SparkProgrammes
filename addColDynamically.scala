import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{lit}

object addColDynamically {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("addColDynamically").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val sourceDF = Seq(("funny", "joke")).toDF("First", "Second")
    sourceDF.show()
    Seq("A","B","C").foldLeft(sourceDF)((df,col)=>{df.withColumn(col.toString,lit(0))}).show()
    spark.close()
  }
}
