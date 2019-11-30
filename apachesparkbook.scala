import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object apachesparkbook {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","apachesparkbook")

    println("\n combineByKey \n")

    val combineByKeyInput=sc.parallelize(Seq(
                          ("maths", 50), ("maths", 60),
                          ("english", 65),
                          ("physics", 66), ("physics", 61), ("physics", 87)),
      3)

    println(combineByKeyInput.getNumPartitions)

    val combineByKeyOutput=combineByKeyInput.combineByKey(
      mark=>{
        println("Create Combiner "+mark)
        (mark,1)
      },
      (acc:(Int,Int),v)=>{
        println("Merge value (%d + %d,%d+1)".format(acc._1,v,acc._2))
        (acc._1+v,acc._2+1)
      },
      (acc1:(Int,Int),acc2:(Int,Int) )=> {
      println("Merge Combiner (%d + %d,%d+%d)".format(acc1._1,acc2._1,acc2._2,acc2._2))
        (acc1._1+acc2._1,acc2._2+acc1._2)
      }
    )
    combineByKeyOutput.foreach(x=>{println(x)})

    val combineByKeysResult=combineByKeyOutput.mapValues(x=>{x._1.toDouble/x._2})
    combineByKeysResult.foreach(x=>{println(x)})

    println("\n Accumulator \n")
    val inputRDD=sc.textFile("C:\\Users\\PLaxmikant\\Documents\\SourceCode\\PROC_UTIL\\wbauto\\ora_wb_stop.sh",5)

    val blanklines=sc.longAccumulator("blanklines")

    val fileParse=inputRDD.foreach(x=>{
      if(x.startsWith("if")){
        blanklines.add(1)
      }
    })

    println("BlankLines: "+blanklines.value)

    println(combineByKeyInput.partitions.size)

    /*
    |     * This is a seqOp for merging T into a U
    |     * ie (String, Int) in  into Int
    |     * (we take (String, Int) in 'value' & return Int)
    |     * Arguments :
    |     * acc   :  Reprsents the accumulated result
    |     * value :  Represents the element in 'inputrdd'
    |     *          In our case this of type (String, Int)
    |     * Return value
    |     * We are returning an Int
    |     */
    /*
     |     * This is a combOp for mergining two U's
     |     * (ie 2 Int)
     |     */

    println("\n AggregateOut \n")
    val aggOutput=combineByKeyInput.aggregate(3)((acc,value)=>{
      println("%s %d+%d ".format(value._1,acc,value._2))
      acc+value._2},
      (acc1,acc2)=>{
        println("%d+%d".format(acc1,acc2))
        acc1+acc2})

    println(aggOutput)

    println("\n ReduceOutPut \n")
    val reduceOutPut=combineByKeyInput.map(x=>{x._2}).reduce((x,y)=>{x+y})
    println(reduceOutPut)

    println("\n FoldValues \n")
    val FoldValuesOut=combineByKeyInput.map(x=>{x._2}).fold(3)((x,y)=>{
      println("%d+%d".format(x,y))
      x+y}) //zero value acts on each partation
    println(FoldValuesOut)

    println("\n Fold RDD \n")

    val ZeroVal=("Zero",3)

    val FoldRddOut=combineByKeyInput.fold(ZeroVal)((acc,marks)=>{
      println("%d+%d".format(acc._2,marks._2))
      ("total",acc._2+marks._2)
    })

    println(FoldRddOut)

    println("\n CountByValues \n")
    val countByValsOut=combineByKeyInput.map(x=>{x._1})countByValue()
    println(countByValsOut)

val SampleRecords=combineByKeyInput.takeSample(true,3,2)

    SampleRecords.foreach(x=>{println(x)})
    println("\n Top \n")
    combineByKeyInput.top(3).foreach(x=>{println(x)})
    println("\n lookup \n")
    combineByKeyInput.lookup("physics").foreach(x=>{println(x)})

    val rdd1 = sc.parallelize(List("lion", "tiger", "tiger", "peacock", "horse"))
    val rdd2 = sc.parallelize(List("lion", "tiger"))

    println("\n Distinct \n")
    rdd1.distinct().foreach(x=>{println(x)})
    println("\n Union \n")
    rdd1.union(rdd2).foreach(x=>{println(x)})
    println("\n Intersect \n")
    rdd1.intersection(rdd2).foreach((x=>{println(x)}))
    println("\n Substract \n")
    rdd1.subtract(rdd2).foreach(x=>{println(x)})
    println("\n cartasian \n")
    rdd1.cartesian(rdd2).foreach(x=>{println(x)})
    println("\n GroupByKey \n")
    combineByKeyInput.groupByKey().glom().foreach(x=>{println(x.mkString(","))})
    println("\n foldByKey \n")
    combineByKeyInput.foldByKey(3)((x,y)=>{x+y}).foreach(x=>{println(x)})
    println("\n GroupBy \n")
    combineByKeyInput.groupBy(x=>{
      if(x._2%2==0){
        "even"
      }
      else {
        "odd"
      }
    }).foreach(x=>{println(x._1,x._2.mkString(" "))})

    val KeyRdd1=sc.parallelize(Seq(("key1", 1), ("key2", 2),("key1", 3),("key5", 7)))
    val keyRdd2=sc.parallelize(Seq(("key1", 5), ("key2", 4),("key4", 6)))
    KeyRdd1.cogroup(keyRdd2).foreach(x=>{println(x._1,x._2)})

    val JoinRDD1=sc.parallelize(Seq(("math",55),("math",56),("english",57),("english",58),("science",59),("science",54)))
    val joinRDD2= sc.parallelize(Seq(("math",60),("math",65),("science",61),("science",62),("history",63),("history",64)))

    println("\n Join \n")
    JoinRDD1.join(joinRDD2).foreach(x=>{println(x)})
    println("\n LeftJoin \n")
    JoinRDD1.leftOuterJoin(joinRDD2).foreach(x=>{println(x)})
    println("\n RightJoin \n")
    JoinRDD1.rightOuterJoin(joinRDD2).foreach(x=>{println(x)})
    println("\n FullJoin \n")
    JoinRDD1.leftOuterJoin(joinRDD2).foreach(x=>{println(x)})
    println("\n GroupWith \n")
    JoinRDD1.groupWith(joinRDD2).foreach(x=>{println(x)})
    sc.stop()

  }
}
