import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
object UdemyLinerRegressionDataSet {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val confg=new SparkConf().setAppName("UdemyLinerRegressionDataSet").setMaster("local[*]")

    val spark=SparkSession.builder().config(confg).getOrCreate()

    val sc=spark.sparkContext

    val inputLines=sc.textFile("D:\\SparkScalaUdemy\\regression.txt")

    // Load up our page speed / amount spent data in the format required by MLLib
    // (which is label, vector of features)

    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.

val data=inputLines.map(x=>{x.split(",")})
    .map(x=>{(x(0).toDouble,Vectors.dense(x(1).toDouble))})

    /* Convert this RDD to a DataFrame */
    import spark.implicits._
    val colNames=Seq("label","features")
    val df=data.toDF(colNames:_*)

    // Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    // Perhaps you're importing data from a real database. Or you are using structured streaming
    // to get your data.

    // Let's split our data into training data and testing data

    val trainTest=df.randomSplit(Array(0.5,0.5))
    val trainTestDF=trainTest(0)
    val testDF=trainTest(1)

    // Now create our linear regression model
val lir=new LinearRegression()
    .setRegParam(0.3) //regulrization
    .setElasticNetParam(0.8) //elestic net mixing
    .setMaxIter(100) //max iteraton
    .setTol(1E-16)

    // Train the model using our training data
val model=lir.fit(trainTestDF)

    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our
    // test dataframe:

    val fullPredection=model.transform(testDF).cache()

    // This basically adds a "prediction" column to our testDF dataframe.

    // Extract the predictions and the "known" correct labels.

    val predectionAndLabel=fullPredection.select("prediction", "label")
        .rdd.map(x=>{(x.getDouble(0),x.getDouble(1))})

    // Print out the predicted and actual values for each point
for(predection <- predectionAndLabel ){
  println(predection)
}
    sc.stop()
    spark.stop()
  }
}
