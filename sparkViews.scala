
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
object sparkViews {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("sparkViews").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val marksData = spark.read.option("header", "true")
      .option("inferschema","true")
      .csv("/Users/pl465j/Downloads/marks.csv").as("marksData")
    marksData.printSchema()
    marksData.show(truncate = false)
    val localView="LocalView"
    marksData.createOrReplaceTempView(localView)
    spark.sql("select * from %s where ROLL_NO=1".format(localView)).show()

    val globalView="GlobalView"
    marksData.createOrReplaceGlobalTempView(globalView)

    val catalog = spark.catalog
    catalog.listDatabases().show(truncate = false)
    catalog.listTables("default").show()
    spark.sql("show tables from default").show(truncate = false)
    spark.sql("show tables from global_temp").show(truncate = false)

    spark.sql("select * from global_temp.%s where ROLL_NO=2".format(globalView)).show()

    val newSpark=spark.newSession()

    println("GTT")
    newSpark.sql("select * from global_temp.%s where ROLL_NO=2".format(globalView)).show()

    //newSpark.sql("select * from %s where ROLL_NO=1".format(localView)).show()

    println(spark)
    println(newSpark)

    newSpark.stop()
    newSpark.close()

    spark.stop()
    spark.close()

  }

}
