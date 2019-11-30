import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object UdemySparkSQL {

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String):Person={
    val fields = line.split(",")

    return Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("UdemeySparkSQL").setMaster("local[*]")

    val spark= SparkSession.builder().config(conf).getOrCreate()

    val sc=spark.sparkContext

    val lines=sc.textFile("D:\\SparkScalaUdemy\\fakefriends.csv")

    val friends=lines.map(x=>{mapper(x)})
    friends.foreach(x=>{println(x)})

    import spark.implicits._

    val schemaFriedsDataSet=friends.toDS

    val schemaFriendsDataFrame=friends.toDF

    println("\n DataSet \n")
    println(schemaFriedsDataSet)
schemaFriedsDataSet.printSchema()

    println("\n DataFrame \n")
    println(schemaFriendsDataFrame)
    schemaFriendsDataFrame.printSchema()

//create a view
    schemaFriedsDataSet.createOrReplaceTempView("FriendsView")
    val teenages=spark.sql("select * from FriendsView where age >12 and age <20")

    println("\n Teenagers  \n")
    teenages.foreach(x=>{println(x)})

    val Teenage=schemaFriedsDataSet.filter($"age" > "12" && $"age" < "20")

    println("\n Teenage \n")
    Teenage.foreach(x=>{println(x.age,x.ID,x.name,x.numFriends)})

    Teenage.select("Name","age").show()

    println("\n Friends count By Age \n")
    schemaFriedsDataSet.groupBy("age").count().orderBy("age").show()

    println("\n Added Age \n")
    schemaFriedsDataSet.select(schemaFriedsDataSet("name"),schemaFriedsDataSet("age") +10).show()
sc.stop()
    spark.stop()
  }

}
