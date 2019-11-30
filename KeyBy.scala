import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession

object KeyBy {

  def keyByF(t:Int) : Int={
    return  t*t
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val confg=new SparkConf().setMaster("local[*]").setAppName("KeyBy")
    val spark=SparkSession.builder().config(confg).getOrCreate()
    val sc=spark.sparkContext

    val rdd=sc.parallelize(Array(1,2,3,4,5,6))
    rdd.foreach(x=>{println(x)})
    val newRdd=rdd.keyBy(x=>{keyByF(x)})
    newRdd.foreach(x=>{println(x)})
    sc.stop()
    spark.stop()
  }
}
