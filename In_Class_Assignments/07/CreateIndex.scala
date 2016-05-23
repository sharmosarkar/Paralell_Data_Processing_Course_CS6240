import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

// Author : Sharmodeep Sarkar

object CreateIndex {

	def main(args: Array[String]) {

		val conf = new SparkConf().

		setAppName("MyIndexer").

		setMaster("local")

		val sc = new SparkContext(conf)

        val teamsDataset = sc.textFile("baseball/Teams.csv").

        map { _.split(",") }.

        filter { _(0) != "yearID" }.

        map (data => (data(0) + "," + data(2), data(40)))

        val appearancesDataset = sc.textFile("baseball/Appearances.csv").

        map { _.split(",") }.

        filter { _(0) != "yearID" }.

        map (data => (data(0) + "," + data(1), data(3)))

        val playersDataset = sc.textFile("baseball/Master.csv").

        map { _.split(",") }.

        filter { _(0) != "playerID" }.

        map (data => (data(0), (data(13) ,data(14))))

        val dataFrame1 = appearancesDataset.join(teamsDataset)

        val frameInfo1 = dataFrame1.map(x => { 
                                        val (k,v) = x
                                        val y = k.split(",")(0)
                                        val (p,h) = v
                                        (p,(h, y))}) 
        val dataFrame2 = playersDataset.join(frameInfo1)
        val frameInfo2 = dataFrame2.map(y => { 
                                        val (k,v) = y
                                        val (p,h) = v
                                        val (a, b) = h
                                        val (name1, name2) = p
                                        k + "," + name1 +" "+ name2 + "," + a + "," +b})
        frameInfo2.saveAsTextFile("index-output")
        // Shut down Spark, avoid errors.
        sc.stop()
            }
        }

                // vim: set ts=4 sw=4 et:
