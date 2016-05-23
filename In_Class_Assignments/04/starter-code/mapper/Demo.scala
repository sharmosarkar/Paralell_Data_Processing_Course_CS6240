package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

// Author: Nat Tuck
object Demo {
    def main(args: Array[String]) {
        println("Demo: startup")

        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        job.setMapperClass(classOf[DemoMapper])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])

        FileInputFormat.addInputPath(job,  new Path("data.csv"))
        FileOutputFormat.setOutputPath(job, new Path("out"))


        // Actually run the thing.
        job.waitForCompletion(true)
    }
}

// Author: Nat Tuck
class DemoMapper extends Mapper[Object, Text, Text, Text] {
    type Context = Mapper[Object, Text, Text, Text]#Context

    var maps = 0

    override def setup(ctx: Context) {
        println("DemoMapper.setup")
    }

    override def cleanup(ctx: Context) {
        println("DemoMapper.cleanup")
        println(s"DemoMapper maps: $maps")
        ctx.write(new Text("count"), new Text("" + maps));
    }

    override def map(_k: Object, line: Text, ctx: Context) {
        maps += 1
    }
}

// vim: set ts=4 sw=4 et:
