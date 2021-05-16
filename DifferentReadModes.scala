import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}
object DifferentReadModes {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("DifferentReadModes").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val dataDF=spark.read.option("header","true").option("inferSchema","true")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
      .format("csv").load("/Users/pl465j/Downloads/invalidEmps.csv")
     dataDF.show()
    val permissiveDF=spark.read.option("header","true").
      option("mode","PERMISSIVE").option("inferSchema","true")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .format("csv").load("/Users/pl465j/Downloads/invalidEmps.csv")
    permissiveDF.select($"*").show()
    val dropMalformed=spark.read.option("header","true").option("mode","dropMalformed")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .format("csv").load("/Users/pl465j/Downloads/invalidEmps.csv")
    dropMalformed.show()

    val dfReadCsv=spark.read.option("header","true").option("inferSchema","true").
    option("mode","PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
      .csv("/Users/pl465j/Downloads/invalidEmps.csv")
    dfReadCsv.show()
    //val df = spark.read.csv('/tmp/inputFile.csv', header=True, schema=dataSchema, enforceSchema=True)


    try {
      val dropFailFast=spark.read.option("header","true").option("mode","failfast").format("csv").load("/Users/pl465j/Downloads/invalidEmps.csv")
      dropFailFast.show()
    }
    catch {
      case e:Exception => println("Error creating DF {}".format(e.getMessage))
    }

    spark.stop()
    spark.close()
  }
}


/*

3 Read Modes
PERMISSIVE -> Takes everything , puts NULL to wrong schema
FAILFAST -> Throws exception for corrupt record
DROPMALFORMED -> Removes the records that does not match schema
 */