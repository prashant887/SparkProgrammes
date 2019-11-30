
import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
object SparkHiveContext {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("SparkKafkaIntegration").setMaster("local[*]")

    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val sc=spark.sparkContext
    import spark.implicits._

    val RawRdd=sc.textFile("D:\\data-master\\data-master\\cards\\smalldecks\\deckofcards.txt")

    val CardsData=RawRdd.map(x=>{x.split("\\|")}).map(x=>{(x(0),x(1),x(2))})

    //CardsData.foreach(x=>{println(x)})

    val CardsDF=CardsData.toDF("Color","Type","Number")

    //CardsDF.show()
    CardsDF.createOrReplaceTempView("CardsView")

    spark.sql("select Color,Type,Number from CardsView").show()

    val schema=StructType(Seq(StructField("Color",StringType,true)
      ,StructField("Type",StringType,true),StructField("Number",StringType,true)))

    println("\n Using DataFrame \n")
    val DataFrame=spark.read.format("csv")
      .schema(schema)
      .option("delimiter","|")
      .option("nullValue","N/A")
      .option("header","false")
      .option("path","D:\\data-master\\data-master\\cards\\smalldecks\\deckofcards.txt")
      .load().as("CardsDataFrame")

    DataFrame.show()

    sc.stop()
    spark.stop()
  }
}
