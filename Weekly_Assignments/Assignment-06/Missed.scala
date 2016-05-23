
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.Sorting._
import scala.collection.immutable.ListMap
import java.text.SimpleDateFormat
import java.util.regex._

// Author : Sharmo and Sarita
object Missed {
    // constants
        val YEAR = 0
        val DATE = 5
        val AL_CARRIER = 6
        val ORIGIN = 11//14
        val DESTINATION = 20//23
        val AC_ARR_TIME = 41
        val AC_DEP_TIME = 30
        val SC_ARR_TIME = 40
        val SC_DEP_TIME = 29
        val CANCEL_FLG = 47
        val FLT_NO = 10
        val TAIL_NO = 9
    // consts used only in sanity checks
        val CRS_ELAPSED_TIME = 50
    // time is in mins
        val CONNECTION_TIME_UPPER_lIMIT = 6*60
        val CONNECTION_TIME_LOWER_LIMIT = 30

        val sdf = new SimpleDateFormat("yyyy-MM-dd") 

    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Missed!!").
            setMaster("local")
        val sc = new SparkContext(conf)               
             
        // Input
        val master = sc.textFile(args(0)).
            map (x => {
                 x.replace(", ", "").
                 replace("\"", "").
                 split(",")
            }).
            filter (x=>{ x(0) != "YEAR" && !Flight_Data_Sanity_Check.sanityTest(x)}).
            keyBy ( _(AL_CARRIER))

/*        
        val tmp = master.map(x=>{   
            val (k,v) = x
            v.length+"--"+v.mkString(":")
        })
        
*/
        val arrivingFLT = master.map(x=>{
                val (key,value) = x ;
                val k = value(AL_CARRIER) + "-" + value(DESTINATION)
                val v = value(TAIL_NO)+value(FLT_NO)+":" +value(SC_ARR_TIME) + ":" +value(AC_ARR_TIME)+":"+value(CANCEL_FLG)+":"+value(DATE)+":"+value(YEAR)
                k -> v
            })

        val departingFLT = master.map(x=>{
                val (key,value) = x ;
                val k = value(AL_CARRIER) + "-" + value(ORIGIN)
                val v = value(TAIL_NO)+value(FLT_NO)+":" +value(SC_DEP_TIME) + ":" +value(AC_DEP_TIME)+":"+value(CANCEL_FLG)+":"+value(DATE)+":"+value(YEAR)
                k -> v
            })


        // Join
        val data = arrivingFLT.join(departingFLT)

        
        // Output from join
        val joinResult = data.map(x => { 
 
            val (k, v) = x;
            var  connections = 0
            var  missedConnections = 0
            // arr = arriving part
            // dep = departing part
            val (arr, dep) = v;
            val  arrival = arr.split(":")
            val  departure = dep.split(":")
            val  arrDate =  sdf.parse(arrival(4))
            val  depDate =  sdf.parse(departure(4))
            var  schtimeDiff = 0
            var  acTimeDiff  = 0
            var  diffDays : Long = 0
            val  key = ( k.split("-")(0) , arrival(5) )

            // if arriving flight and departing flight are on the same day
            if(depDate.equals(arrDate)){
                schtimeDiff = toMins(departure(1)) - toMins(arrival(1))
                if (isConnected(schtimeDiff)){
                    connections = connections +1 

                    acTimeDiff = toMins(departure(2)) - toMins(arrival(2))
                    if (isMissed(acTimeDiff , arrival(3) , departure(3)))
                        missedConnections = missedConnections + 1
                }                
                
            }
            // else the departing flight should be the very next day after the arriving flight
            else {
                diffDays = (depDate.getTime()- arrDate.getTime())/(1000*60*60*24)
                if (diffDays == 1){

                    schtimeDiff =  toMins(departure(1))+(24*60) - toMins(arrival(1))
                    if (isConnected(schtimeDiff)){
                        connections = connections +1 

                        acTimeDiff = toMins(departure(2))+(24*60) - toMins(arrival(2))
                        if (isMissed(acTimeDiff , arrival(3) , departure(3)))
                            missedConnections = missedConnections + 1
                    }                
                }
            }
           
           

            //key -> connections + "\t" + missedConnections + "\t" + ((missedConnections/connections)*100)
            key -> ( connections , missedConnections)
        })

        //val finalResult = joinResult.reduceByKey(_+_)
        val reducedRes = joinResult.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2) ).collect()
        
        val finalResult = sc.parallelize(reducedRes).map (z => { 
                                            val (k,v) = z
                                            val (connections,missedconnections) = v 
                                            val outputVal = (connections,missedconnections,((1.0*missedconnections/connections*100)+"%"))
                                            k -> outputVal
                                                })
        
        finalResult.saveAsTextFile(args(1))

        // Shut down Spark, avoid errors.
        sc.stop()
    }


    // check if a valid connection exists
    def isConnected(timeDiff : Int) : Boolean = {
        return ( timeDiff <= CONNECTION_TIME_UPPER_lIMIT && timeDiff >= CONNECTION_TIME_LOWER_LIMIT )
    }

    // check if a connection was missed
    def isMissed(timeDiff : Int , arrCancelledBit: String , depCancelledBit: String) : Boolean = {
        return ( timeDiff < CONNECTION_TIME_LOWER_LIMIT || arrCancelledBit.toInt == 1|| depCancelledBit.toInt == 1)
    }


    // converts hhmm or hmm to mins 
    def toMins(time : String) : Int = {
        //if (time.length < 2 || time.length > 4)
        //    return -1
        
        if (time.length == 2)
            return  time.toInt
        if (time.length == 3)
            return (time.substring(0,1).toInt)*60 + (time.substring(1,3).toInt)
        else
            return (time.substring(0,2).toInt)*60 + (time.substring(2,4).toInt)        
    }

}

// vim: set ts=4 sw=4 et:
