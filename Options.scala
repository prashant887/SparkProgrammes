object Options {

  def sommeOfNegativeInts(l: List[Option[Int]]): Int = {

    l.map(x=>{x.getOrElse(0)}).filter(x=>{x<0}).reduce((x,y)=>{x+y})

}
  def main(args: Array[String]): Unit = {

    println(sommeOfNegativeInts(List[Option[Int]](Some(-1), None, Some(2))))

  }
}
