name:="pagerank"

version := "1.0"

scalaVersion := "2.10.5"

val sparkC = "org.apache.spark" %% "spark-core"  % "1.5.2"

val sparkM = "org.apache.spark" %% "spark-mllib" % "1.5.2"
 
libraryDependencies ++= Seq(
  sparkC,
  sparkM
)