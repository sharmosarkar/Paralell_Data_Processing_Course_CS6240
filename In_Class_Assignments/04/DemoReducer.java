package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
    private int reduces ;
    private String sample ;

    @Override
    protected void setup(Context ctx) {
        reduces = 0;
        sample="";
        System.out.println("DemoReducer.setup");
    }

    @Override
    // Authors : Sarita and Sharmodeep
    protected void cleanup(Context ctx) {
        System.out.println("DemoReducer.cleanup");
        //System.out.println("There are: " + reduces + " words starting with letters like "  + sample.charAt(0));
        if(sample.matches("[a-z].*"))
            System.out.println("There are: " + reduces + " words starting with letters like "  + "[a-z]");
        else if(sample.matches("[A-Z].*"))
            System.out.println("There are: " + reduces + " words starting with letters like "  + "[A-Z]");
        else
            System.out.println("There are: " + reduces + " words starting with letters like "  + "[.,_]  (non-letter characters)");


    }

    // Authors : Sarita and Sharmodeep
    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        int count = 0;
        
        for (Text xx : vals) {
            count += Integer.parseInt(xx.toString());

        }

        try {
            ctx.write(key, new Text("" + count));
        }
        catch (Exception _e) {
            throw new Error("I give up");
        }

        reduces += 1;
        sample = key.toString();
    }

/*
    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        int count = 0;

        for (Text xx : vals) {
            count += Integer.parseInt(xx.toString());
        }

        try {
            ctx.write(key, new Text("" + count));
        }
        catch (Exception _e) {
            throw new Error("I give up");
        }

        reduces += 1;
    }

*/
}
