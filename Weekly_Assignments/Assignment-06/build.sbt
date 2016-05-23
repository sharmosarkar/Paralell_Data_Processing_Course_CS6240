
lazy val root = (project in file(".")).
    settings(
        name := "Missed",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
		
    )


compileOrder := CompileOrder.JavaThenScala 
mainClass in (Compile, run) := Some("Missed")
mainClass in (Compile, packageBin) := Some("Missed")
