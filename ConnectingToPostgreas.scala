import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
object ConnectingToPostgreas {

  case class client_map(client_id:Int,client_short_name:String,client_long_name:String)
  case class client_job_step_exec_vw(prcs_week:Int,batch_id:Int,client_job_step_exec_id:Int,soltn_abbr:String,
                                     lgcl_dm_abbr:String,client_short_name:String)
  case class client_soltn_prcs_option(client_id:Int,lgcl_dm_id:Int,soltn_id:Int,refresh_frequency:String
                                      ,is_required:Int,spec_id:Int)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("ConnectingPostgreas")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
import spark.implicits._
    val jdbcDS=spark.read.
      format("jdbc")
      .option("url","jdbc:postgresql://lnx1546.ch3.dev.i.com:5432/opsbld")
      .option("dbtable", "opsbld_iri_p1.mdm_client_map")
      .option("user", "opsbld_iri_p1")
      .option("password", "Passw0rd")
      .option("inferSchema","true")
      .load()
      .as[client_map]
        .as("client_map")
    jdbcDS.show()


    val clientjobstecexecvwDs=spark.read.
      format("jdbc")
      .option("url","jdbc:postgresql://lnx1546.ch3.dev.i.com:5432/opsbld")
      .option("query", "select prcs_week::int as prcs_week ,batch_id::int,client_job_step_exec_id::int,soltn_abbr,lgcl_dm_abbr,client_short_name from client_job_step_exec_vw where prcs_week=1998 and lgcl_dm_abbr='UKIRI'")
      .option("user", "opsbld_iri_p1")
      .option("password", "Passw0rd")
      .option("inferSchema","true")
      .load()
      .as[client_job_step_exec_vw]
      .as("client_job_step_exec_vw")
    clientjobstecexecvwDs.show()

    val client_soltnprcsoptionDs=spark.read.
      format("jdbc")
      .option("url","jdbc:postgresql://lnx1546.ch3.dev.i.com:5432/opsbld")
      .option("query", "select client_id::int,lgcl_dm_id::int,soltn_id::int,soltn_prcs_option_id::int,refresh_frequency,is_required::int,spec_id::int from client_soltn_prcs_option")
      .option("user", "opsbld_iri_p1")
      .option("password", "Passw0rd")
      .option("inferSchema","true")
      .load()
      .as[client_soltn_prcs_option]
      .as("client_soltn_prcs_option")
    client_soltnprcsoptionDs.filter($"is_required"===1).show()

val JoindDF=client_soltnprcsoptionDs.
  join(jdbcDS,$"client_soltn_prcs_option.client_id"===$"client_map.client_id","inner")
  .filter($"client_soltn_prcs_option.is_required"===1)
    .select($"client_soltn_prcs_option.spec_id".alias("spec_id"),$"client_map.client_short_name".alias("Client_name"))

    JoindDF.show()

    JoindDF.write.saveAsTable("SampleOutTable")
    sc.stop()
    spark.stop()
  }
}
