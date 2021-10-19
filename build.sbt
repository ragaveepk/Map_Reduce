name := "WordCount"

version := "0.1"

scalaVersion := "2.13.6"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


libraryDependencies ++=Seq(
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"

)
mainClass in(Compile, run) := Some("MapperReducer")
mainClass in assembly := Some("MapperReducer")
assemblyJarName in assembly := "Homework2.jar"

//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
