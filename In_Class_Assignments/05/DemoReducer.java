package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.lang.*;

// Author: Nat Tuck
// Modified by : Sharmodeep Sarkar
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        String homeRuns = null;
        String salary = null;

        for (Text xx : vals) {
            String[] parts = xx.toString().split("=");

            if (parts[0].equals("homeRuns")) {
                homeRuns = parts[1];
            }
            
            if (parts[0].equals("salary")) {
                salary = parts[1];
            }

        }

        try {
            if (homeRuns != null && salary != null) {
                Double hr = Double.parseDouble(homeRuns);
                Double sal = Double.parseDouble(salary); 
                Double val = sal/hr*1.0 ;
                ctx.write(key, new Text(val+""));
            }
        }
        catch (Exception ee) {
            ee.printStackTrace(System.err);
            throw new Error("I give up");
        }
    }
}
