import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}

object SpeculativeExecution {

  case class  EMP(id:Int,Name:String){
    if (id==5)
      {
        val sleepConfig=50
        println("Will sleep for %d seconds".format(sleepConfig))
        Thread.sleep(sleepConfig*1000)
      }
      Thread.sleep(2)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SpeculativeExecution").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val marksData = sc.makeRDD(1 to 1000).map(x=>{EMP(x,"Employee Name %s".format(x))}).toDF()
    marksData.show()
    sc.stop()
    spark.stop()
    spark.close()

  }

  /*if there is any slpw running taks , spark will reluanch same tasks in different executor parallely
  when tasks is completed in exe-2. other tasks will be killed in exe-1

  params
  spark.speculations = True
  spark.speculation.interval=200
  spark.speculation.multiplier=5
  spark.speculation.quantile=0.75

  Total Task = 4

  completed task(3)-3ms
  Pending task(1) ... 75% completed so spulation kicks in

   */
}
