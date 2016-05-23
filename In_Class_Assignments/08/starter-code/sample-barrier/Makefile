
barrier.jar: Barrier.java
	sbt assembly
	mv target/scala-*/*barrier*jar barrier.jar

clean:
	rm -rf *.jar project/target project/project target

.PHONY: clean
