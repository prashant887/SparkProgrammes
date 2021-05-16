import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object unionsTypes {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("unionsTypes").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val dataOne=Seq(("Avni","Bangalore","37373"),
      ("Ganesh","Mangalore","2332"),
      ("Bala","Patna","24553"))

    val dataOneDF=spark.createDataFrame(dataOne).toDF("Name","Place","Number")

    dataOneDF.show(truncate = false)

    val dataTwo=Seq(("Mumbai","33442","Mahesh","SucessFul"),
      ("Bellary","114455","Avi","Fail"),
      ("Delhi","12333","Ranveer","Sucessful"))

    val dataTwoDF=spark.createDataFrame(dataTwo).toDF("Place","Number","Name","Status")

    dataTwoDF.show(truncate = false)

    val dataThree=Seq(("Mumbai","33442","Mahesh"),
      ("Bellary","114455","Avi"),
      ("Delhi","12333","Ranveer"))

    val dataThreeDF=spark.createDataFrame(dataThree).toDF("Place","Number","Name")

    dataThreeDF.show(truncate = false)

    dataOneDF.union(dataThreeDF).show(truncate = false)

    dataOneDF.unionByName(dataThreeDF).show(truncate = false)

    dataOneDF.unionByName(dataTwoDF,allowMissingColumns = true).show(truncate = false)


    spark.stop()
    spark.close()


  }

}
