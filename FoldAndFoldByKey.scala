import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object FoldAndFoldByKey {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","FoldAndFoldByKey")

    val RddFold=sc.parallelize(List(1,2,3,4,5,6,7))

    val FoldOut=RddFold.fold(0)((x,y)=>{x+y})

    println("\n Foldout = %d \n".format(FoldOut))

    val RddFoldByKey=sc.parallelize(Array(
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

    val rddFoldByKeyOut=RddFoldByKey.map(x=>{(x._1,x._3.toInt)})
      .foldByKey(0,1)((x,y)=>{x+y})

    rddFoldByKeyOut.foreach(x=>{println(x)})

    val CmpmyKey=RddFoldByKey.map(x=>{(x._1,x._3.toInt)})
        .combineByKey(((x:Int)=>(x,1)),
          ((x:(Int,Int),y)=>(x._1+y,x._2+1)),
          ((x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2))
        )

    CmpmyKey.foreach(x=>{println(x)})


    sc.stop()
  }

}
