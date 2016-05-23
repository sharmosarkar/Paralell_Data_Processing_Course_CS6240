#!/bin/bash
javac myjava.java
START=$(date +%s)
java myjava
END=$(date +%s)
DIFF=$(( $END - $START ))
echo $DIFF >> mytext
