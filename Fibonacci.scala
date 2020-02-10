object Fibonacci {

  def fibonacci(n:Int):Long={
    /*if (n==0){
      return 0
    }
*/
    if (n==1||n==2){
      return 1
    }

//return fibonacci(n-1)+fibonacci(n-2)

    var fib1=1
    var fib2=1
    var fibfinal=1
    for (i <- 3 to n) {
      fibfinal=fib1+fib2
      fib1=fib2
      fib2=fibfinal
    }
    return fibfinal
  }
  def main(args: Array[String]): Unit = {

    println(fibonacci(3))

  }
}
