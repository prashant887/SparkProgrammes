import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{regexp_replace,when,udf}

object BucketPurning {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("BucketPurning").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val seq=(1 to 10000).toSeq.map(x=>{(x,"Name of x %d".format(x))})


    val df=spark.createDataFrame(seq).toDF("number","name")
    df.show()
    println(df.rdd.getNumPartitions)
    df.coalesce(1).write.mode("overwrite").saveAsTable("Unbucketed")
   df.coalesce(1).write.bucketBy(4,"number")
      .sortBy("number").mode("overwrite")
      .saveAsTable("Bucketed")

    spark.sql("select * from Unbucketed where number=1000").explain()
    spark.sql("select * from Bucketed where number=1000").explain()
    spark.sql("Describe extended Bucketed").show()




  }

}
