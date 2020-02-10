object FunctionalBasics {

  def sommeOfCubeOfNegativeElements(l: List[Int]): Int = {

    return l.filter(x=>{x<0}).map(x=>{x*x*x}).reduce((x,y)=>x+y)
  }
  def main(args: Array[String]): Unit = {

    println(sommeOfCubeOfNegativeElements(List[Int](-1, 0, 6)))


  }
}
