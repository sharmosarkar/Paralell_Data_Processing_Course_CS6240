//Author: Sharmodeep Sarkar

lazy val root = (project in file(".")).
    settings(
        name := "Index",
        libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core" % "1.5.2",
		"org.apache.spark" % "spark-sql_2.10" % "1.6.0"))
