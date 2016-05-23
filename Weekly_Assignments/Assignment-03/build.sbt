// Author: Nat Tuck

lazy val root = (project in file(".")).
  settings(
    name := "Cluster_Analysis_Average",
    version := "1.0",
    mainClass in Compile := Some("main.Cluster_Analysis_Average")
  )

// Scala Runtime
//libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.0"
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
