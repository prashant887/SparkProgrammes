import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

import org.apache.log4j.Logger
import org.apache.log4j.Level

object FirstSparkApp {
  def main(args: Array[String]): Unit = {

    //disable log4j messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //create a Spark Config Object and Spark Context Object to initalize spark
    val conf=new SparkConf() //Entry point to the programme
    conf.setMaster("local") //Local Machine
    conf.setAppName("First Spark App") //App Name to show in URL

    val sc=new SparkContext(conf)

      // entry point build for DataSet and DataFrame APIs called as SparkSession
      val sparkSession=SparkSession.builder.master("local").appName("First Spark App")
        //.enableHiveSupport()
        .getOrCreate()
  //  val sc=sparkSession.con
    import sparkSession.implicits._ //to use $ with column name
    val sqlConext=new SQLContext(sc)
      //Create RDD

      val rdd1=sc.makeRDD(Array(1,2,3,4,5,6,7,8))
    rdd1.collect().foreach(x=>{println(x)})

    val textFile=sc.textFile("D:\\hrrebuilddependency.txt")
    textFile.collect().foreach(x=>{println(x)})

    val partationFile=sc.textFile("D:\\62ConstellationInteractive.txt",5)
    partationFile.collect().foreach(x=>{println(x)})
    partationFile.foreachPartition(x=>{println("Number of Items in paration - %d".format(x.count(y=>true)))})

      //Create Dataframe

      val df=sparkSession.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .option("nullValue","NA")
        .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
        .option("mode","failfast")
        .option("path","D:\\mental-health-in-tech-survey\\survey.csv")
        .load()

      df.select("Timestamp","Age","remote_work","leave").filter("Age>30").show()

    val df1 = df.select( "Gender","treatment")
    df1.show()

    val df2 = df1.select($"Gender",
      (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
      (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
    )

    df2.show()

    val df3=df2.groupBy($"Gender").agg(sum($"All-Yes"),sum($"All-Nos"))

    df3.show()

    //Create UDF to handle Geneder Standardization

    def parseGender(g: String) = {
      g.toLowerCase match {
        case "male" | "m" | "male-ish" | "maile" |
             "mal" | "male (cis)" | "make" | "male " |
             "man" | "msle" | "mail" | "malr" |
             "cis man" | "cis male" => "Male"
        case "cis female" | "f" | "female" |
             "woman" |  "femake" | "female " |
             "cis-female/femme" | "female (cis)" |
             "femail" => "Female"
        case _ => "Transgender"
      }
    }

    val parseGenderUDF = udf(parseGender _)

    val df4=df2.select((parseGenderUDF($"Gender")).alias("Gender"),
      $"All-Yes",
      $"All-Nos"
    )

    df4.show()

    val df5=df4.groupBy($"Gender").agg(sum($"All-Yes").alias("Yes"),sum($"All-Nos").alias("No"))
    df5.show()

    val df6=df5.filter($"Gender"=!="Transgender")

    df6.collect

    df6.show()

    //Spark SQL

    //Create Schema

    import org.apache.spark.sql.types._

    val surveySchema = StructType(Array(StructField("timestamp",TimestampType,true),
      StructField("age",LongType,true),
      StructField("gender",StringType,true),
      StructField("country",StringType,true),
      StructField("state",StringType,true),
      StructField("self_employed",StringType,true),
      StructField("family_history",StringType,true),
      StructField("treatment",StringType,true),
      StructField("work_interfere",StringType,true),
      StructField("no_employees",StringType,true),
      StructField("remote_work",StringType,true),
      StructField("tech_company",StringType,true),
      StructField("benefits",StringType,true),
      StructField("care_options",StringType,true),
      StructField("wellness_program",StringType,true),
      StructField("seek_help",StringType,true),
      StructField("anonymity",StringType,true),
      StructField("leave",StringType,true),
      StructField("mental_health_consequence",StringType,true),
      StructField("phys_health_consequence",StringType,true),
      StructField("coworkers",StringType,true),
      StructField("supervisor",StringType,true),
      StructField("mental_health_interview",StringType,true),
      StructField("phys_health_interview",StringType,true),
      StructField("mental_vs_physical",StringType,true),
      StructField("obs_consequence",StringType,true),
      StructField("comments",StringType,true))
    )

    val sql_df=sparkSession.read.format("csv")
        .schema(surveySchema)
        .option("header","true")
        .option("nullValue","NA")
        .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
        .option("mode","failfast")
        .load("D:\\mental-health-in-tech-survey\\survey.csv")
    println(sql_df.schema)

    //create temp table or view

    sql_df.createOrReplaceTempView("survery_tbl")

    sparkSession.catalog.listTables.show

    sql_df.createOrReplaceGlobalTempView("survery_gtbl") //global tables belongs to database called global_temp

    sparkSession.catalog.listTables("global_temp").show()

    df6.write.format("parquet")
      .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
      .mode("overwrite")
      .save("D:\\mental-health-in-tech-survey\\parquere_output")


    df6.write.format("json")
      .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
      .mode("overwrite")
      .save("D:\\mental-health-in-tech-survey\\json_output")

    df.createOrReplaceTempView("survey")
/*sparkSession.sql("""select age,count(*) as frequency from survey where age between 20 and 65 group by age""")
      .write.mode("overwrite")
      .saveAsTable("survery_frequency")


    sparkSession.catalog.listTables.show

    sparkSession.sql("""select * from survery_frequency""").show()*/
    sparkSession.sql("""select * from survey""").show()
    //UDF should always be in JVM language

    sparkSession.udf.register("pgender",(s:String)=>if(List("f","female","women").contains(s.toLowerCase))"Female" else "Male")
    sparkSession.sql("""select pgender('f') """).show()
    sparkSession.sql("""select pgender("man")""").show()

val word_count=sc.textFile("D:\\serde.txt")
    word_count.flatMap(x=>{x.split(" ")}) //split sencence based on space
      .map(x=>{(x.replaceAll("\"",""),1)}) //assing count of 1(val) to each word(key)
      .reduceByKey((x,y) => {x+y}) //add all the values/count for each word/key
      .collect()
      .foreach(x=>{println(x)})
    println("Number of Lines in Serde file %d".format(word_count.count()))

    println("===========Without Split================")
    //word_count.flatten.collect().foreach(x=>{println(x)})
    sparkSession.stop()
  }


}
