object AlphaSum {

  def getPoints(ints: List[Char]):Unit={

    val charlist= List[Char] ('A','B','C','D')
    var total=0.0
    for (i <- ints ) {
      var as=i.toInt
      var intial=1
      var pos=charlist.indexOf(as)
      var points=intial+math.pow(2,pos)
      total=total+points
    }

  }
  def main(args: Array[String]): Unit = {
getPoints(List[Char] ('A','B','Z'))

  }
}
