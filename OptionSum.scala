object OptionSum {

  def addsum(l: List[Option[Int]]): Int = {

    l.map(x=>{x.getOrElse(0)}).reduce((x,y)=>{x+y})

  }
  def main(args: Array[String]): Unit = {

    println(addsum(List[Option[Int]](Some(-1), None, Some(2))))

  }
}
