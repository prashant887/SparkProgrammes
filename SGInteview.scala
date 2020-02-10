object SGInteview {

  def main(args: Array[String]): Unit = {
    val lst:List[Int]=List.empty

    try
      {
        lst.reduce((x,y)=>{x+y})
      }
    catch {
      case unknown => println(unknown)
    }
    println(lst.fold(0)((x,y)=>{x+y}))

    val ScalaMap=Map(1 -> "One",2 -> "Two")

    println(ScalaMap.get(1).getOrElse(0))
    println(ScalaMap.get(3).getOrElse(0))

    val NewLst=List[Int](1,2,-3,5,-7)
    println(NewLst.foldRight(1)((x,y)=>{x/y}))
    println(NewLst.foldLeft(1)((x,y)=>{x/y}))
  }



}
