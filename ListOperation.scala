object ListOperation {

  def separateStringBy(strList: Seq[String], delimiter: String): String = {

    strList.mkString(delimiter)

  }
  def main(args: Array[String]): Unit = {

    val strList = Seq("S1", "S2", "S3")

    println(separateStringBy(strList, ","))


  }
}
