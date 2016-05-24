name:="stream_first"

version := "1.0"

scalaVersion := "2.10.5"

val sparkC = "org.apache.spark" %% "spark-core"  % "1.5.2"

val sparkM = "org.apache.spark" %% "spark-mllib" % "1.5.2"

val breezeB  =  "org.scalanlp" %% "breeze" % "0.12"
val breezeBN =  "org.scalanlp" %% "breeze-natives" % "0.12"

libraryDependencies ++= Seq(
  sparkC,
  sparkM,
  breezeB,
  breezeBN
)