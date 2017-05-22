name := "extract"

version := "1.0"

scalaVersion := "2.12.1"


val akkaVersion = "2.5.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)