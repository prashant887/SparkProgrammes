import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SparkSession,Row}
import org.apache.spark.sql.types.{StructField,StringType,StructType,IntegerType,ArrayType,MapType}
import org.apache.spark.sql.functions.{explode,explode_outer,posexplode,posexplode_outer}
object Explode {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("Explode").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )


    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)

    df.select($"name",explode($"knownLanguages")).show(false)

    df.select($"name",explode($"properties")).show(false)

    df.select($"name",explode_outer($"knownLanguages")).show(false)

    df.select($"name",explode_outer($"properties")).show(false)

    df.select($"name",posexplode($"knownLanguages")).show(false)

    df.select($"name",posexplode($"properties")).show(false)

    df.select($"name",posexplode_outer($"knownLanguages")).show(false)

    df.select($"name",posexplode_outer($"properties")).show(false)
    sc.stop()
    spark.stop()
  }

}
