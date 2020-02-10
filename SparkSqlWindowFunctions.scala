import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object SparkSqlWindowFunctions {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("SparkSqlWindowFunctions").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    val df2=spark.createDataFrame(simpleData).toDF("employee_name", "department", "salary")
    //df.show()

    val windowSpec=Window.partitionBy("department").orderBy("salary")
    val RowNum=df.withColumn("RowNum",row_number over windowSpec  )
    RowNum.show()
    val Rank=df.withColumn("rank",rank.over(windowSpec))
    Rank.show()
    val DenseRank=df.withColumn("DenseRank",dense_rank() over windowSpec)
    DenseRank.show()
    println(df2.groupBy("department").count())
    spark.catalog.listTables().show()
    sc.stop()
    spark.stop()
  }

}
