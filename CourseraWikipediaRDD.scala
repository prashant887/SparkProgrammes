import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object CourseraWikipediaRDD {

  case class Wikipedia(title:String,text:String){
    def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
  }

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  def parser(line:String):Wikipedia={
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    Wikipedia(title, text)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf().setAppName("CourseraWikipediaRDD").setMaster("local[*]")
  val spark=SparkSession.builder().config(conf).getOrCreate()
  val sc=spark.sparkContext

  val wikiRDD:RDD[Wikipedia]=sc.textFile("D:\\Couresera\\wikipedia.dat").map(x=>{parser(x)})

  def occurrencesOfLang(lang:String,rdd:RDD[Wikipedia]):Int={
    val res=rdd.aggregate(0)(
      (acc,page)=>{ if (page.text.toLowerCase.contains(lang.toLowerCase()))
        {
          acc+1
        }
        else {acc}
      }
       ,
      (acc1,acc2)=>{acc1+acc2}
    )
    return res
  }
def rankLang(langs:List[String],rdd:RDD[Wikipedia]):List[(String,Int)]={
  val res=langs.map(x=>{(x,occurrencesOfLang(x,rdd))})
    .sortBy(x=>{-x._2})
return res

}
  //:RDD[(String,Iterable[Wikipedia])]
  def makeIndex(langs:List[String],rdd:RDD[Wikipedia]):RDD[(String,Iterable[Wikipedia])]={
    val articleLanguagePair=rdd.flatMap(artical=>{
      val langMentioned=langs.filter(lang=>{artical.text.toLowerCase.split(" ").contains(lang.toLowerCase)})
      langMentioned.map(x=>{(x,artical)})
    })
    articleLanguagePair.groupByKey()
  }
  def main(args: Array[String]): Unit = {
rankLang(langs,wikiRDD).foreach(x=>{println(x)})
    makeIndex(langs,wikiRDD).foreach(x=>{println(x)})
  }
}
