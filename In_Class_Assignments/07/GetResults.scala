
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

// Author : Sharmodeep Sarkar
object GetQueryResults {
	def main(args: Array[String]) {
		val conf = new SparkConf().
		setAppName("CreateIndex").
		setMaster("local")
		val sc = new SparkContext(conf)


                            Console.println("Please enter the PLAYERID whose details are to be retrieved .. ")
                            val query = Console.readLine()

                            val master = sc.textFile("index-output/part-00000").
                            map { _.split(",") }.
                            filter { x=> ( x(0) != "playerID" && x(0) == query)}.
                            map (p => {
                                    p(0) -> (p(1) , p(2) , p(3))
                                })
                

                            val grouped = master.groupByKey
                            val sorted = grouped.mapValues(x => x.toList.sortBy(_._3))
                                                    
                            val res = sorted.map(x=>{
                                val (k,v) = x 
                                v.map( y => {
                                     y
                                    })
                                }).
                                flatMap( y => y)


                        res.saveAsTextFile("final-output")

                        // Shut down Spark, avoid errors.
                        sc.stop()
                    }
                }

                // vim: set ts=4 sw=4 et:
