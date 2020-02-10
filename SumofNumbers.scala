
object SumofNumbers {

  def sumofNumbers(ints: List[Int]):Int={
    var sum=0

    for (i<-ints) {
      sum=sum+i
    }
    return sum
  }
  def main(args: Array[String]): Unit = {

    println(sumofNumbers(List[Int](1,2,3,4,5)))


  }
}
