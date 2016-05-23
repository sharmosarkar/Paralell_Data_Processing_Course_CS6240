
/* PROBLEM STATEMENT ::::
Write a Spark program that finds the (player, year) that hit the most home runs per dollar of salary.
*/ 


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.Sorting._
import scala.collection.immutable.ListMap

// Author :: Nat Tuck
// Modified By :: Sharmodeep Sarkar

object qs1 {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Hall of Fame").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val salaries = sc.textFile("baseball/Salaries.csv").
            map { _.split(",") }.
            filter { _(3) != "playerID" }.
            keyBy (x=> {x(3)+" "+x(0) })

        val batting = sc.textFile("baseball/Batting.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x=> {x(0)+" "+x(1) })

        // Join
        val data = batting.join(salaries)
        
        // Output
        val text = data.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            if(p.length>=11 && h(4).toFloat>0 && p(11).toFloat>0){
                    val vals = p(11).toFloat/h(4).toFloat
                    val key = p(0)+" in the year "+h(0)+" hits "
                    key + "," + vals
            } else { "N/A--dummyVal" + "," + "-9999"}
        })
		
        val sorted_text = text.collect().sortWith(_.split(",")(1).toFloat > _.split(",")(1).toFloat).take(1)
        val finalVal = sc.parallelize(sorted_text)

        println("\n TOP EARNER \n")
        sorted_text.foreach(x=>{println(x+ " homeruns per $ of salary ")})
        println("\n")        

        finalVal.saveAsTextFile("out")
        //s.saveAsSequenceFile("out")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
