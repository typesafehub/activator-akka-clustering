name := "akka-clustering"

version := "0.1"

scalaVersion := "2.11.7"

lazy val akkaVersion = "2.4.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.1.6" % "test")

