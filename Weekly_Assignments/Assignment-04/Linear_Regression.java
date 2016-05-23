import java.io.*;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Sharmo and Sarita
 */
public class Linear_Regression {
    
    
    public static class FlightTicketMapper extends Mapper<Object, Text, Text, Text> {

    private String[] reformatInputValue(String ip){
    	ip = ip.replace(", ", "");    
        ip = ip.replace("\"", "");      
        return (ip.split(","));
    }
    
    public void map(Object offset, Text record, Context context) throws IOException, InterruptedException {
        
        Text key = new Text();
        Text value = new Text(); 

        String[] line = reformatInputValue (record.toString());

        boolean record_corrupt = false;
        
        //check if the current record is a valid record or a corrupt record
        record_corrupt = sanityTest(line);
        if (line != null && !record_corrupt){
            String v = line[0] + "-" + line[52] + "-"+ line[54] + "-"+ line[109];
            String k = line[6];
            key.set(k);
            value.set(v);
            context.write(key, value);            
        }        
        
    }
    
    
    // Returns true if record is corrupt else returns false
    private boolean sanityTest(String[] input) {
		// entry corrupt indicator
		boolean corrupt = false;
                int ss = 0;
                if (input.length!=110)
                    return true;
                
                // checking if month and year are in valid format (else record is corrupt)
                try{
                     //int month = Integer.parseInt(input[2]);
                     int year = Integer.parseInt(input[0]);
                     float price = Float.parseFloat(input[109]);
                     int airTime = Integer.parseInt(input[52]);
                     int distanceTravelled = Integer.parseInt(input[54]);
                     //int divertedDistance = Integer.parseInt(input[68]);
                }catch (Exception e){
                    return true;
                }
                
		int CRS_DEP_TIME, CRS_ARR_TIME, CRS_ELAPSED_TIME;
		int OAirportID, OAirportSeqID, OCityMarketID, OStateFips, OWAC;
		int DAirportID, DAirportSeqID, DCityMarketID, DStateFips, DWAC;
		int ArrTime, DepTime, ActualElapsedTime, checkdiff;
		int timezone, crs_diff, actual_diff;
		int CANCELLED_FLIGHT_INDICATOR;

		float ARR_DELAY, ARR_DELAY_NEW, ARR_DEL15;

		String ORIGIN, ORIGIN_CITY_NAME, ORIGIN_STATE_ABR, ORIGIN_STATE_NM;
		String DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEST_STATE_NM;

		boolean atleastOneAlpha = input[29].matches(".*[a-zA-Z]+.*");
                
                // BEGIN NEW ADDITION FOR HW01
                boolean yearHasAlpha = input[0].matches(".*[a-zA-Z]+.*");
                boolean monthHasAlpha = input[2].matches(".*[a-zA-Z]+.*");
                // assuming that year and month column shouldnt have any alphabets and can't be empty
                if (yearHasAlpha || monthHasAlpha || input[0].isEmpty() || input[2].isEmpty()){
                    corrupt = true;
                    return corrupt;
                }
                // END OF NEW ADDITION FOR HW01

		// check if CRS_DEP_TIME contains characters
		// if it contains, then record is marked as corrupt
		if (atleastOneAlpha) {
			corrupt = true;
			return corrupt;
		}

		// check if CRS_DEP_TIME is empty
		// if it is empty, then record is marked as corrupt
		if (input[29].isEmpty()||input[40].isEmpty() || input[52].isEmpty() || input[54].isEmpty()) {
			corrupt = true;
			return corrupt;
		}

		CRS_ARR_TIME = Integer.parseInt(input[40]);
		CRS_DEP_TIME = Integer.parseInt(input[29]);

		// compute difference of CRS_ARR_TIME, CRS_DEP_TIME
		// The if else clause is for handling the case when CRS_ARR_TIME is less
		// than CRS_DEP_TIME
		if (CRS_ARR_TIME > CRS_DEP_TIME) {
			int h = (Integer.parseInt(input[40].substring(0, 2)) - Integer.parseInt(input[29].substring(0, 2))) * 60;
			int m = Integer.parseInt(input[40].substring(2, 4)) - Integer.parseInt(input[29].substring(2, 4));
			crs_diff = h + m;
		} 
                else {
			int h = (Integer.parseInt(input[40].substring(0, 2)) - Integer.parseInt(input[29].substring(0, 2)) + 24)
					* 60;
			int m = Integer.parseInt(input[40].substring(2, 4)) - Integer.parseInt(input[29].substring(2, 4));
			crs_diff = h + m;
		}

		CRS_ELAPSED_TIME = Integer.parseInt(input[50]);
		OAirportID = Integer.parseInt(input[11]);
		OAirportSeqID = Integer.parseInt(input[12]);
		OCityMarketID = Integer.parseInt(input[13]);
		OStateFips = Integer.parseInt(input[17]);
		OWAC = Integer.parseInt(input[19]);

		DAirportID = Integer.parseInt(input[20]);
		DAirportSeqID = Integer.parseInt(input[21]);
		DCityMarketID = Integer.parseInt(input[22]);
		DStateFips = Integer.parseInt(input[26]);
		DWAC = Integer.parseInt(input[28]);

		ORIGIN = input[14];
		ORIGIN_CITY_NAME = input[15];
		ORIGIN_STATE_ABR = input[16];
		ORIGIN_STATE_NM = input[18];

		DEST = input[23];
		DEST_CITY_NAME = input[24];
		DEST_STATE_ABR = input[25];
		DEST_STATE_NM = input[27];

		// CANCELLED_FLIGHT_INDICATOR is 1 if flight is cancelled
		CANCELLED_FLIGHT_INDICATOR = Integer.parseInt(input[47]);

		if (input[30].isEmpty()||input[41].isEmpty()) {
			corrupt = false;
			return corrupt;
		}

		ArrTime = Integer.parseInt(input[41]);
		DepTime = Integer.parseInt(input[30]);

		// compute difference of ArrTime, DepTime
		// The if else clause is for handling the case when ArrTime is less than
		// DepTime
		if (ArrTime > DepTime) {
			int h = (Integer.parseInt(input[41].substring(0, 2)) - Integer.parseInt(input[30].substring(0, 2))) * 60;
			int m = Integer.parseInt(input[41].substring(2, 4)) - Integer.parseInt(input[30].substring(2, 4));
			actual_diff = h + m;
		} 
                else {
			int h = (Integer.parseInt(input[41].substring(0, 2)) - Integer.parseInt(input[30].substring(0, 2)) + 24)
					* 60;
			int m = Integer.parseInt(input[41].substring(2, 4)) - Integer.parseInt(input[30].substring(2, 4));
			actual_diff = h + m;
		}

		ARR_DELAY = Float.parseFloat(input[42]);
		ARR_DELAY_NEW = Float.parseFloat(input[43]);
		ARR_DEL15 = Float.parseFloat(input[44]);
		ActualElapsedTime = Integer.parseInt(input[51]);

		// Values that are used in Sanity Test -IV
		timezone = crs_diff - CRS_ELAPSED_TIME;
		checkdiff = actual_diff - ActualElapsedTime - timezone;

		// Sanity Test -I
		// CRSArrTime and CRSDepTime should not be zero
		if (CRS_DEP_TIME == 0 && CRS_ARR_TIME == 0) {
			ss = ss + 1;
			//System.out.println(ss);
			corrupt = true;
			return corrupt;
		}

		// Sanity Test -II
		// timeZone % 60 should be 0
		if (timezone % 60 != 0) {
			corrupt = true;
			return corrupt;
		}
		// Sanity Test -III
		// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
		// larger than 0
		// Origin, Destination, CityName, State, StateName should not be empty
		if (OAirportID <= 0 || OAirportSeqID <= 0 || OCityMarketID <= 0 || OStateFips <= 0 || DAirportID <= 0
				|| DAirportSeqID <= 0 || DCityMarketID <= 0 || DStateFips <= 0) {
			corrupt = true;
			return corrupt;
		}
		if (ORIGIN.isEmpty() || ORIGIN_CITY_NAME.isEmpty() || ORIGIN_STATE_ABR.isEmpty() || ORIGIN_STATE_NM.isEmpty()) {
			corrupt = true;
			return corrupt;
		}
		if (DEST.isEmpty() || DEST_CITY_NAME.isEmpty() || DEST_STATE_ABR.isEmpty() || DEST_STATE_NM.isEmpty()) {
			corrupt = true;
			return corrupt;
		}
		// Sanity Test -IV
		// For flights that not Cancelled:
		// ArrTime - DepTime - ActualElapsedTime - timeZone should be zero
		// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
		// if ArrDelay < 0 then ArrDelayMinutes should be zero
		// if ArrDelayMinutes >= 15 then ArrDel15 should be false
		if (CANCELLED_FLIGHT_INDICATOR == 0) {
			if (checkdiff != 0) {
				corrupt = true;
				return corrupt;
			}
			if (ARR_DELAY > 0 && (ARR_DELAY != ARR_DELAY_NEW)) {
				corrupt = true;
				return corrupt;
			}
			if (ARR_DELAY < 0 && ARR_DELAY_NEW != 0) {
				corrupt = true;
				return corrupt;
			}
			if (ARR_DELAY_NEW >= 15 && ARR_DEL15 != 1) {
				corrupt = true;
				return corrupt;
			}
		}
		// if a record passes all above tests, then value is set to false as the
		// record is sane record
		corrupt = false;
		return corrupt;
	}
    
    
  }
    
    
	// Reducer Class
	public static class FlightTicketReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Boolean isCarrierActiveFlag = false;
			ArrayList<String> tmp = new ArrayList<String>();
			Text reducerValue = new Text();

			for (Text xx : values) {
				String ip = xx.toString();
				String valueArr[] = ip.split("-");
				int year = Integer.parseInt(valueArr[0]);
				if(year>= 2010 && year<=2014) {
					String reducerValueString = valueArr[1] + "\t" + valueArr[2] + "\t" + valueArr[3];
					tmp.add(reducerValueString);
				}
				if(year == 2015)
					isCarrierActiveFlag = true;
			}

			if(isCarrierActiveFlag){
				for(String v : tmp){
					reducerValue.set(v);
					context.write(key, reducerValue);
				}
			}			

		}
	}
    
    
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Flight-Ticket-Price");
    job.setJarByClass(Linear_Regression.class);
    job.setNumReduceTasks(1);
    job.setMapperClass(FlightTicketMapper.class);
    //job.setCombinerClass(FlightTicketReducer.class);
    job.setReducerClass(FlightTicketReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }   
}