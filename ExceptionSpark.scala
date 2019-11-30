//import java.io.{FileNotFoundException, IOException}

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object ExceptionSpark {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","ExceptionSpark")

    try {
      val rdd=sc.textFile("D:\\emp.xlsx")
      rdd.foreach(x=>{println(x)})
    }
    catch {
      //case e:FileNotFoundException => {println("Couldnot find file "+e)}
     // case e:IOException => {println("Some IO Exception occured "+e)}
      case e:Throwable => {println(e.getMessage) }
    }
    finally {
      println("Finished Executuion")
    }
  }
}
