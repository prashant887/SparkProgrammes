import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import  org.apache.spark.sql.types.{StructField,StructType,IntegerType,StringType,TimestampType,FloatType}
import org.apache.spark.sql.functions.{substring,dayofmonth,month,year,concat,lpad,asc,desc}
object ChiagaoCrimeRate {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("ChiagoCrime")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    val crimeRdd=sc.textFile("D:\\Crimes_-_2019.csv")

    val crimeTypePerMonth=crimeRdd.map(x=>{x.split(",")})
      .filter(x=>{x(0).matches("\\d+")})
        .map(x=>{
          val mmddyyy=x(2).substring(0,10).split("/")
          val yyyymm=mmddyyy(2).concat(mmddyyy(0)).toInt
          ((yyyymm,x(5)),1)})
        .reduceByKey((x,y)=>{x+y})
        .sortBy(x=>{(x._1._1,-x._2)})
      .map(x=>{(x._1._1+","+x._1._2+","+x._2)})
        .collect()

    println("\n Using RDD \n")
    crimeTypePerMonth.foreach(x=>{println(x)})

    println("\n Using DataFrmae \n")

    import spark.implicits._

    val schema=StructType(Seq(
      StructField("ID",IntegerType,true),
      StructField("CaseNumber",StringType,true),
      StructField("Date",TimestampType,true),
      StructField("Block",StringType,true),
      StructField("ICUR",StringType,true),
      StructField("PrimaryType",StringType,true),
      StructField("Description",StringType,true),
      StructField("LocationDescription",StringType,true),
      StructField("Arrest",StringType,true),
      StructField("Domestic",StringType,true),
      StructField("Beat",StringType,true),
      StructField("District",StringType,true),
      StructField("Ward",StringType,true),
      StructField("CommunityArea",StringType,true),
      StructField("FBICode",StringType,true),
      StructField("XCoordinate",StringType,true),
      StructField("YCoordinate",StringType,true),
      StructField("Year",StringType,true),
      StructField("UpdatedOn",TimestampType,true),
      StructField("Latitude",StringType,true),
      StructField("Longitude",StringType,true),
      StructField("Location",StringType,true),
      StructField("HistoricalWards",StringType,true),
      StructField("ZipCodes",StringType,true),
      StructField("CommunityAreas",StringType,true),
      StructField("CensusTracts",StringType,true),
      StructField("Wards",StringType,true),
      StructField("Boundaries-ZIPCodes",StringType,true),
      StructField("PoliceDistricts",StringType,true),
      StructField("PoliceBeats",StringType,true)
    ))

    val CrimeDataFrame=spark.read.format("csv")
      .schema(schema)
      .option("header","true")
      .option("nullValue","NA")
      .option("timestampFormat","MM/dd/yyyy HH:mm:ss a")
      .option("mode","failfast")
      .option("path","D:\\Crimes_-_2019.csv")
      .load()

   // CrimeDataFrame.printSchema()
    //CrimeDataFrame.show()
    //CrimeDataFrame.select(year(CrimeDataFrame("Date")).alias("Year"),month(CrimeDataFrame("Date")).alias("Month"),CrimeDataFrame("PrimaryType")).show()
val DateTypeDF=CrimeDataFrame.select(concat(year($"Date"),lpad(month($"Date"),2,"0")).alias("YearMonth"),$"PrimaryType")

    println("\n CrimeCount Using DataFrame \n")
val CrimeCountDF=DateTypeDF.groupBy($"YearMonth",$"PrimaryType")
      .count()
      .withColumnRenamed("count","CrimeCount")
   //   .sort(asc("YearMonth"),desc("CrimeCount"))
    .sort($"YearMonth".asc,$"CrimeCount".desc)

    CrimeCountDF.show()
    /*
    CrimeDataFrame.createOrReplaceTempView("ChiagaoCrimeTable")
    spark.sql("select Date,PrimaryType,count(*) cnt from ChiagaoCrimeTable group by Date,PrimaryType order by Date,cnt desc").show()


    val DfSchema=StructType(List(StructField("date",StringType,true),StructField("type",StringType,true)))

    val crimeDF=spark.createDataFrame(crimeRdd.map(x=>{x.split(",")})
        .map(x=>{val yyyymmdd=x(2).substring(1,10).split("/")
        val mmyyyy=yyyymmdd(0).concat(yyyymmdd(1)).toInt
          (mmyyyy,x(5))
        })
      ,DfSchema)*/

    println("\n Using SQL \n")
    val crimeDF=crimeRdd.map(x=>{x.split(",")})
      .filter(x=>{x(0).matches("\\d+")})
      .map(x=>{val yyyymmdd=x(2).substring(0,10).split("/")
        val mmyyyy=yyyymmdd(2).concat(yyyymmdd(0)).toInt
        (mmyyyy,x(5))
      }).toDF().withColumnRenamed("_1","date").withColumnRenamed("_2","Type")

    //crimeDF.printSchema()
    //crimeDF.show()

    crimeDF.createOrReplaceTempView("crimeDFTbl")

  val CrimeOutSqlDF=  spark.sql("select date,Type,count(*) cnt from crimeDFTbl group by date,Type order by date,cnt desc")
    CrimeOutSqlDF.show()

    val DateTypeFilteredDF=CrimeDataFrame
      .filter($"LocationDescription"==="RESIDENCE")
        .select($"PrimaryType")
        .groupBy($"PrimaryType").count().withColumnRenamed("count","CrimeTypeCount")
        .sort($"CrimeTypeCount".desc)

    DateTypeFilteredDF.show()

    sc.stop()
    spark.stop()

  }
}
