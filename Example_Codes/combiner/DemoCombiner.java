package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoCombiner extends Reducer<Text, Text, Text, Text> {
    private int reduces;
    private int valsSeen;

    @Override
    protected void setup(Context ctx) {
        reduces = 0;
        valsSeen = 0;

        System.out.println("DemoCombiner.setup");
    }

    @Override
    protected void cleanup(Context ctx) {
        System.out.println("DemoCombiner.cleanup");
        System.out.println("Reduces: " + reduces);
        System.out.println("Vals seen: " + valsSeen);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        int count = 0;

        for (Text xx : vals) {
            count += Integer.parseInt(xx.toString());
            valsSeen += 1;
        }

        try {
            ctx.write(key, new Text("" + count));
        }
        catch (Exception _e) {
            throw new Error("I give up");
        }

        reduces += 1;
    }
}
