name := """xRay"""

organization := "Vishnu"
version := "0.7"
scalaVersion := "2.11.8"

// Dependencies
libraryDependencies ++= Seq (
  "com.typesafe.akka" %% "akka-stream" % "2.4.16",
  "com.amazonaws" % "aws-java-sdk"  % "1.11.87",
  "org.specs2" %% "specs2" % "3.7",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.parquet" % "parquet-avro" % "1.9.0",
  "org.apache.avro" % "avro" % "1.8.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "com.typesafe.akka" %% "akka-stream" % "2.4.16",
  "com.typesafe.akka" %% "akka-http" % "10.0.4",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.4",
  "com.github.scopt" %% "scopt" % "3.5.0"
  )

// Scala compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-explaintypes",
  "-encoding", "UTF-8",
  "-Xlint"
  )


enablePlugins(JavaAppPackaging)

fork in run := true
