import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object CombineByKeyFunction {

  def createScoreCombiner(x:String):(Double,Int)={
    return (x.toDouble,1)
  }

  def mergeValue(acc:(Double,Int),elem:(String,Int)):(Double,Int)={
    return(acc._1+elem._1.toDouble,acc._2+1)
  }

  def mergeCombiner(acc1:(Double,Int),acc2:(Double,Int)):(Double,Int)={
    return (acc1._1+acc2._1,acc1._2+acc1._2)
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

val sc=new SparkContext("local[*]","CombineByKeyFunction")
    val studentRDD = sc.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
      ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
      ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
      ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
      ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
      ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
      ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
      ("Juan", "Biology", 60)), 3)

    val GrpByVal=studentRDD.groupBy(x=>{x._1})

    GrpByVal.foreach(x=>{println(x)})

    val mappedVal=studentRDD.map(x=>{(x._1,x._3)})
    /*    .combineByKey((x:Int)=>(x,1),
          (acc:(Int,Int),elem:(Int,Int))=>(acc._1+elem._1,acc._2+1),
          (acc1:(Int,Int),acc2:(Int,Int)) => (acc1._1+acc2._1,acc2._2+acc1._2)
        )*/



    mappedVal.foreach(x=>{println(x)})
    sc.stop()

  }
}
