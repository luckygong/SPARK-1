name:= "sql_spark"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4"

libraryDependencies += "org.apache.spark" %% "spark-core"    % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql"     % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx"  % "1.5.1"
