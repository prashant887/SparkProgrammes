object ScalaOption {

  case class Emp(EmpId:Int,Ename:String,Comm:Option[Int],Mgr:Option[String])

  def main(args: Array[String]): Unit = {
    val ary=Array(Emp(1,"Abc",Some(100),Some("xyz")),Emp(2,"def",None,None))

    ary.foreach(x=>{println(x)})

    for (i <- 1 to 10) {
      println(i)
    }
  }
}
