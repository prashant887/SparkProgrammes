import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
object TreeReduce {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("TreeReduce").setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext

    val Rdd=sc.parallelize(Seq(1,2,3,4,5,6))
    val out=Rdd.treeReduce((x,y)=>{x+y},2)

    /*

    xy.treeAggregate(0.0)(
        seqOp = (c, v) => {
          (c + ((v._1 - v._2) * (v._1 - v._2)))
        },
        combOp = (c1, c2) => {
          (c1 + c2)
        })
     */
    println(out)
    sc.stop()
    spark.stop()
  }
}
