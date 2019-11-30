import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object MapVsMapWithPartation {

  def parseFile(line:String):(String,String,String,String)={
    val fileds=line.split(",")

    return (fileds(0),fileds(1),fileds(2),fileds(3))
  }

  def parseFileItr(itr:Int,lines:Iterator[String]):Iterator[(String,String,String,String)]={

return    lines.toList.map(x=>{x.split(",")}).map(x=>{(x(0),x(1),x(2),x(3))}).iterator

    }

  def f(partitionIndex:Int, i:Iterator[Int],k:List[Int]) = {


    (partitionIndex,k.iterator.sum ,k).productIterator
  }





  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc=new SparkContext("local[*]","MapVsMapWithPartation")

    val RawFile=sc.textFile("D:\\smallmoviereating.csv",4)

    val ReadMap=RawFile.map(x=>{parseFile(x)})

    println("\n ReadMap \n ")
ReadMap.foreach(x=>{println(x)})

    val ReadMapWithPartIdx=RawFile.mapPartitionsWithIndex((idx,rec)=>{rec.map(x=>{(idx,parseFile(x))})})

    println("\n ReadMapWithPartIdx \n")
    ReadMapWithPartIdx.foreach(x=>{println(x)})


    val PrationSize=RawFile.mapPartitionsWithIndex((idx,rec)=>{Array((idx,rec.size)).iterator})
      .sortByKey(true)
        .collect()

    println("\n size total %d \n ".format(RawFile.count()))

    PrationSize.foreach(x=>{println(x)})

    val GlomPartation=RawFile.glom()

    println("\n Glom Partation \n")

    GlomPartation.foreach(x=>{ println(x.length+"=="+ x.mkString(","))})

    val x= sc.parallelize(Array(1,2,3,4,5,6,7,8,9), 4)

    val MapIdx=x.mapPartitionsWithIndex((idx,rec)=>{(f(idx,rec,rec.toList))})
      .glom()
      .map(x=>{(x(0).toString.toInt,(x(1),x(2)))})
        .sortByKey()
        .collect()
    println("\n Fun Partation \n")

    MapIdx.foreach(x=>{println(x)})

    val set1=sc.parallelize(Array(2,4,6,8),3)
    val set2=sc.parallelize(Array(1,5,7,9),2)

    val union=set1.union(set2).glom().map(x=>{x.mkString(",")})

    println("\n Union \n")
    union.foreach(x=>{println(x)})

    val part1=sc.parallelize(Array(("a",1),("a",2),("b",4),("d",5)))
    val part2=sc.parallelize(Array(("a",3),("a",4),("b",7),("c",8)))

    val joined=part1.join(part2)
      .glom()
        .map(x=>{x.mkString(",")})

    println("\n Joined \n")

    joined.foreach(x=>{println(x)})

    val reduced=x.coalesce(2).glom().map(k=>{k.mkString(",")})

    println("X size %d Reduced size %d".format(x.partitions.size,reduced.partitions.size))

    reduced.foreach(x=>{println(x)})

    val names= sc.parallelize(Array("John", "Fred", "Anna", "James","John"))

    val groupBYKeys=names.groupBy(x=>{x.charAt(0)})

    println("\n groupByKeys \n")

    groupBYKeys.foreach(x=>{println(x)})

    val byKeys=names.keyBy(x=>{x.charAt(0)})

    println("\n ByKeys \n")

    byKeys.foreach(x=>{println(x)})

    val WordsFile=sc.textFile("C:\\Users\\PLaxmikant\\Downloads\\pagecounts-20160802-040000\\pagecounts-20160802-040000")

    val MapPartation=WordsFile.mapPartitions(lines => {lines.
      flatMap(x=>{x.split("\\W+").map(m=>{(m,1)})})}).reduceByKey((a,b)=>{a+b})
    MapPartation.foreach(x=>{println(x)})

    val rdd=sc.parallelize(List("yellow","red","blue","cyan","black"),3)

    val mapped=rdd.mapPartitionsWithIndex((index, iterator)=>{
      println("Called in Partation "+index)
      val myList=iterator.toList
      myList.map(x => {x + " -> "+index}).iterator
    })

    mapped.foreach(x=>{println(x)})
    sc.stop()

  }
}
