
name := "SparkPersistBenchMark"

version := "0.1"

//scalaVersion := "2.11.8"
scalaVersion := "2.12.11"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" //% "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" //% "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" //% "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" //% "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-launcher
libraryDependencies += "org.apache.spark" %% "spark-launcher" % "3.0.0"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
