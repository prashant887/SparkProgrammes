import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object CombineByKeys {
/*
def createCombiner(score:String):(Double,Int)={
  return (score.toDouble,1)
}

  def mergeValue (acc:(Double,Int),elem:(String,String)):(Double,Int)={
    return (acc._1+elem._2.toDouble,acc._2+1)
  }

  def mergeCombiner(x:(Double,Int),y:(Double,Int)):(Double,Int)={
    return (x._1+y._1,x._2+y._2)
  }
*/

  def main(args: Array[String]): Unit = {

    type ScoreCollector = (Int, Double)
    type PersonScores = (String, (Int, Double))

    val createScoreCombiner = (score: Double) => (1, score)

    val scoreCombiner = (collector: ScoreCollector, score: Double) => {
      val (numberScores, totalScore) = collector
      (numberScores + 1, totalScore + score)
    }

    val scoreMerger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
      val (numScores1, totalScore1) = collector1
      val (numScores2, totalScore2) = collector2
      (numScores1 + numScores2, totalScore1 + totalScore2)
    }
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","GroupByKeys")

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

    val wilmaAndFredScores = sc.parallelize(initialScores).cache()

    val outuput=wilmaAndFredScores.map(x=>{(x._1,createScoreCombiner(x._2))})
        .mapValues(x=>{scoreCombiner(x,x._2)})

    outuput.foreach(x=>{println(x)})

    val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
scores.foreach(x=>{println(x)})

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

    println(">>>> Number of Parataion %d <<<<".format(studentRDD.getNumPartitions))


    studentRDD.foreachPartition(x=>{println(x.mkString(" "))})

    studentRDD.foreachPartition(part=>{part.foreach(
      item=>{println(item)
    })})


    def createCombiner = (tuple: (String, Int)) =>
      (tuple._2.toDouble, 1)

    def mergeValue = (accumulator: (Double, Int), element: (String, Int)) =>
      (accumulator._1 + element._2, accumulator._2 + 1)

    def mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)




      // use combineByKey for finding percentage
    val combRDD = studentRDD.map(t => (t._1, (t._2, t._3)))
        .combineByKey(createCombiner,mergeValue,mergeCombiner)


    combRDD.foreach(x=>{println(x)})

    sc.stop()
  }
}
