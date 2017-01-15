name := "Cichlid"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "org.apache.jena" % "jena-core" % "3.1.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
