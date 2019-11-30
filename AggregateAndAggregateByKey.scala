import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object AggregateAndAggregateByKey {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","AggregateAndAggregateByKey")

    val InitRdd=sc.parallelize(Array(1,2,3,4,5,6,4,5,6,7))


    val agg=InitRdd.aggregate((0,0))(
      (acc,value)=>(acc._1+value,acc._2+1),
      (e1,e2)=>(e1._1+e2._1,e2._2+e2._2)
    )

    println(InitRdd.partitions.size)
    InitRdd.glom().foreach(x=>{println(x.mkString(" "),x.length)})
    println("\n Agg  "+agg + "\n")

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

    RddFoldByKey.takeOrdered(10).foreach(x=>{println(x)})
    val keyRDD = sc.parallelize(List((("a1", "b1"), "c1"), (("a2", "b2"), "c2")), 2)
val swapped=keyRDD.map(x=>{x.swap})
    println("=========DebugString========")
    println(swapped.toDebugString)
    println("======DebugString========")
    swapped.foreach(x=>{println(x)})
    val list: List[((String, String), String)] = List((("a1", "b1"), "c1"), (("a2", "b2"), "c2"))

    val res = list.map { case ((a, b), c) => (a, (b, c)) }

    res.foreach(x=>{println(x)})


    sc.stop()
  }
}
