package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator
// Author: Nat Tuck
object Demo {
    def main(args: Array[String]) {
        println("Demo: startup")

        // Configure log4j to actually log.
        BasicConfigurator.configure();

        // Make a job
        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        // Set classes mapper, reducer, input, output.
        job.setReducerClass(classOf[DemoReducer])

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])

        // Set up number of mappers, reducers.
        job.setNumReduceTasks(2)

        MultipleInputs.addInputPath(job, new Path("baseball/Master.csv"), 
            classOf[TextInputFormat], classOf[NamesMapper])
        MultipleInputs.addInputPath(job, new Path("baseball/HallOfFame.csv"), 
            classOf[TextInputFormat], classOf[FameMapper])
        
        FileOutputFormat.setOutputPath(job, new Path("out"))

        // Actually run the thing.
        job.waitForCompletion(true)
    }
}

// Author: Nat Tuck
class NamesMapper extends Mapper[Object, Text, Text, Text] {
    type Context = Mapper[Object, Text, Text, Text]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        val cols = vv.toString.split(",")
        if (cols(0) == "playerID") {
            return
        }

        val playerID = cols(0)
        val name     = cols(15) + " " + cols(14)

        if (name != null && name != "") {
            ctx.write(new Text(playerID), new Text("name=" + name))
        }
    }
}

class FameMapper extends Mapper[Object, Text, Text, Text] {
    type Context = Mapper[Object, Text, Text, Text]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        val cols = vv.toString.split(",")
        if (cols(0) == "playerID") {
            return
        }

        val playerID = cols(0)
        val hallYear = cols(1)

        if (hallYear != null && hallYear != "") {
            ctx.write(new Text(playerID), new Text("year=" + hallYear))
        }
    }
}

// vim: set ts=4 sw=4 et:
