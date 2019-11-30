name := "SparkProject"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-flume
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3"

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.8"

// https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-flume-sink
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume-sink" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1207.jre7"
