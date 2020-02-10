object SumOfPosNums {

  def sumofPosNumbers(ints: List[Int]):Int={

    return ints.filter(x=>{x>0}).map(x=>{x*x}).reduce((x,y)=>{x+y})
  }
  def main(args: Array[String]): Unit = {

    println(sumofPosNumbers(List[Int](1,-2,-3,4,5)))


  }
}
