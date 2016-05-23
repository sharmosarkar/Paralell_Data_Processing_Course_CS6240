/*
PROBLEM STATEMENT ::
Find the (name, year) of the pitcher (appears in Pitching.csv) with the best batting average (Hits / At Bats).
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.Sorting._
import scala.collection.immutable.ListMap

//Author :: Nat Tuck
// Modified By :: Sharmodeep Sarkar

object qs2 {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Hall of Fame").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val master = sc.textFile("baseball/Master.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy { _(0) }

        val pitch = sc.textFile("baseball/Pitching.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x=> {x(0)+" "+x(1) })

        val batting = sc.textFile("baseball/Batting.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy (x=> {x(0)+" "+x(1) })

        // Join
        val data = batting.join(pitch)
        
        // Output
        val text = data.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            if(p.length>=7 && h(13).toFloat>0 && p(6).toFloat>0 && h.length>=14){
                    val vals = h(13).toFloat/p(6).toFloat
                    val key = p(0)+": in the year "+h(1)+" has the batting average of  "
                    key + "," + vals
            } else { 0.0 + "," + "0.0"}
        })
		
        val sorted_text = text.collect().sortWith(_.split(",")(1).toFloat > _.split(",")(1).toFloat).take(1)
        
        val finalVal = sc.parallelize(sorted_text)

        val finalValMap = finalVal.map( x=> {
            val k = x.split(":")(0)
            k -> x.split(":")(1)
            })

        val joinedModel = finalValMap.join(master.map(x=>{
                val (key,value) = x ;
                // we need only the given name
                key -> value(15)
            }))

        val finalWithName = joinedModel.map(x=>{
            val (k, v) = x;
            val (p, h) = v;
            h.mkString + "  " + p.mkString
        })

        println("\n\n TOP BATTTING AVERAGE \n")
        finalWithName.take(1).foreach(println)
        println("\n")

        finalWithName.saveAsTextFile("out")
        //s.saveAsSequenceFile("out")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
