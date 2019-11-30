import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
object CacheVsPersists {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","CacheVsPersist")

    val PersistRdd=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")

    PersistRdd.persist(StorageLevel.DISK_ONLY)

    val CacheRdd=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")

    CacheRdd.cache()


    val KeyValRdd=PersistRdd.map(x=>{(x.split(",")(0),x)}).filter{case (x,y) => y.length>20}

    KeyValRdd.foreach(x=>{println(x)})
  }
}
