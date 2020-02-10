object Matching {

  def main(args: Array[String]): Unit = {

    val man = Man("Bob")

    whoAmI(man)

  }

  case class Man(name: String)

  case class Woman(name: String)

  case class Dog(name: String)

  case class Cat(name: String)

  def whoAmI(something: Any): Unit =
  {

  val out  =something match {
  case Dog(x) => "My name is "+x+" I am a dog"
  case Man(x) => "My name is "+x+" I am a man"
  case Woman(x) => "My name is "+x+" I am a woman"
  case Cat(x) => "My name is "+x+" I am a cat"
  case _  => "Unknown Speciers"

}

    println(out)

  }
}
