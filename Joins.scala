import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object Joins {

  case class Item(id:String, name:String, unit:Int, companyId:String)
  case class Company(companyId:String, name:String, city:String)
  case class Dept(DeptID:Int,Dept:String,Location:String)
  case class Emp(EmpId:Int, FirstName:String,LastName:String,MgrId:Int,DateOfJoin:String,Salary:Float,comm:Float,DeptId:Int)


  def getEmpClass(line:String):Emp ={
    val filed=line.split(",")

     return Emp(filed(0).toInt,filed(1),filed(2),filed(3).toInt,filed(4),filed(5).toFloat,filed(6).toFloat,filed(7).toInt)
  }

  def getDeptClass(line:String):Dept={

    val fileds=line.split(",")
    return  Dept(fileds(0).toInt,fileds(1),fileds(2))
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc=new SparkContext("local[*]","Joins")

    val i1 = Item("1", "first", 2, "c1")
    val i2 = i1.copy(id="2", name="second")
    val i3 = i1.copy(id="3", name="third", companyId="c2")

    val items = sc.parallelize(List(i1,i2,i3))

    println("\n Items \n")
    items.foreach(x=>{println(x)})

    val c1 = Company("c1", "company-1", "city-1")
    val c2 = Company("c2", "company-2", "city-2")

    val companies = sc.parallelize(List(c1,c2))

    println("\n Compaines \n")
    companies.foreach(x=>{println(x)})

    val groupedItems = items.groupBy( x => x.companyId)

    println("\n Grouped Items \n")

    groupedItems.foreach(x=>{println(x)})

    val groupedComp = companies.groupBy(x => x.companyId)

    println("\n GroupedItem \n")

    groupedComp.foreach(x=>{println(x)})

    val JoinedResult=groupedItems.join(groupedComp)

    println("\n Grouped Result \n")

    JoinedResult.foreach(x=>{println(x)})

    val empData=sc.textFile("D:\\emp.csv")

    val deptData=sc.textFile("D:\\dept.csv")

    val unionData=empData.union(deptData)

    println("\n UnionData \n")

    unionData.foreach(x=>{println(x)})

    println("\n InterSect \n")

    val intersectData=deptData.map(x=>{x.split(",")(0)})
      .intersection(empData.map(x=>{x.split(",")(7)}))

    intersectData.foreach(x=>{println(x)})

    println("\n Cartisian \n")

    val cartisianData=deptData.map(x=>{x.split(",")(0)})
      .cartesian(empData.map(x=>{x.split(",")(0)}))

    cartisianData.foreach(x=>{println(x)})

    println("\n Substract \n")

    val substractData=deptData.map(x=>{x.split(",")(0)})
      .subtract(empData.map(x=>{x.split(",")(7)}))

    substractData.foreach(x=>{println(x)})

    val EmpClassData=empData.map(x=>{getEmpClass(x)})
    val DeptClassData=deptData.map(x=>{getDeptClass(x)})

    println("\n Emp Class Data \n")
    EmpClassData.foreach(x=>{println(x)})

    println("\n Dept Class Data \n")
    DeptClassData.foreach(x=>{println(x)})

val EmpClassMap=EmpClassData.map(x=>{(x.DeptId,x)})
    val DeptClassMap=DeptClassData.map(x=>{(x.DeptID,x)})

    println("\n EmpClass Map \n")
    EmpClassMap.foreach(x=>{println(x)})

    println("\n DeptClass Map \n")
    DeptClassMap.foreach(x=>{println(x)})

    val EmpDeptJoined=EmpClassMap.join(DeptClassMap)

    println("\n Emp Dept Joined \n")
    EmpDeptJoined.foreach(x=>{println(x._2._1.DeptId,x._2._2.DeptID)})

    val LeftOuterJoin=EmpClassMap.leftOuterJoin(DeptClassMap)

    println("\n Left Outer Join \n")

    LeftOuterJoin.foreach(x=>{println(x)})

    val RightOuterJoin=EmpClassMap.rightOuterJoin(DeptClassMap)

    println("\n Right Outer Join \n")
    RightOuterJoin.foreach(x=>{println(x)})

    val FullJoin=EmpClassMap.fullOuterJoin(DeptClassMap)

    FullJoin.foreach(x=>{println(x)})

    val CogroupVal=EmpClassMap.cogroup(DeptClassMap)

    println("\n Co Group Val \n")

    CogroupVal.foreach(x=>{println(x._1+" "+x._2._1.mkString(" ") +" "+x._2._2.mkString(" "))})


      sc.stop()
  }

}
