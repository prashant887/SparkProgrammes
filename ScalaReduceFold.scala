object ScalaReduceFold {

  def main(args: Array[String]): Unit = {
    val lst:List[Int]=List.empty

    try {
      println(lst.reduce((x,y)=>{x+y}))
    }
    catch {
      case e:Exception => println("Error Occured")
    }
    println(lst.fold(0)((x,y)=>{x+y}))
  }

}
