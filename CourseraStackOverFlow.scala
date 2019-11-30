import org.apache.log4j.{Logger,Level}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession

object CourseraStackOverFlow {

  case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

def rawPosting(rdd:RDD[String]):RDD[Posting]= {
  rdd.map(x => {
    val arr = x.split(",")
    Posting(postingType = arr(0).toInt,
      id = arr(1).toInt,
      acceptedAnswer = if (arr(2).equals("")) None else Some(arr(2).toInt),
      parentId = if (arr(3).equals("")) None else Some(arr(3).toInt),
      score = arr(4).toInt,
      tags = if (arr.length >= 6) Some(arr(5).intern()) else None
    )
  })
}
  def groupedPosting(rdd:RDD[Posting]):RDD[(Int,Iterable[(Posting,Posting)])]={
    val answer=rdd.filter(x=>{x.postingType==2 && x.parentId.isDefined})
      .map(x=>{(x.parentId.get,x)})

    val question=rdd.filter(x=>{x.postingType==1})
      .map(x=>{(x.id,x)})

    question.join(answer).groupByKey()
  }

  def scoredPosting(rdd:RDD[(Int,Iterable[(Posting,Posting)])]):RDD[(Posting,Int)]={
    rdd.flatMap(x=>{x._2}).groupByKey().mapValues(x=>{x.map(x=>{x.score}).max})
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("CourseraStackFlow")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    val StackRdd:RDD[String]=sc.textFile("D:\\Couresera\\stackoverflow.csv")
    val raw:RDD[Posting]=rawPosting(StackRdd)
    val grouped=groupedPosting(raw)
    val scored=scoredPosting(grouped)

    raw.take(20).foreach(x=>{println(x)})
    grouped.flatMap(x=>{x._2}).groupByKey()
      .mapValues(x=>{x.map(x=>{x.score}).max})
      .take(20).foreach(x=>{println(x)})
    scored.take(20).foreach(x=>{println(x)})
    sc.stop()
    spark.stop()
  }
}
