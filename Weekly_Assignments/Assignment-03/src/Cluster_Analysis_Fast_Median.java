import au.com.bytecode.opencsv.CSVParser;
import java.io.*;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
public class Cluster_Analysis_Fast_Median {
    
    
    public static class FlightTicketMapper extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private final CSVParser csvParser = new CSVParser(',','"'); 

    public void map(Object offset, Text record, Reducer.Context context) throws IOException, InterruptedException {
        Text key = new Text();
        Text value = new Text();
        String[] line = this.csvParser.parseLine(record.toString());

        boolean record_corrupt = false;
        
        //check if the current record is a valid record or a corrupt record
        record_corrupt = sanityTest(line);
        if (line != null && !record_corrupt){
            String k = getMapOutputKey(line);
            String v = getMapOutputValue(line);
            key.set(k);
            value.set(v);
            if ((key != null) && (value != null)) {
		context.write(key, value);
            }
        }        
        
    }
    

    // the output of this function is the key to the mapper class output (key = airline carrier)
    private String getMapOutputKey(String[] currRow){
        String airline = currRow[6];
        /*
        String month = currRow[2];
        String year = currRow[0];
        StringBuilder keyString = new StringBuilder();
        
        keyString.append(airline).append(",");
        keyString.append(year).append(",");
        keyString.append(month);    */
        return airline;
    }
    
    // the output of this function is the value that is to be set against every carrier
    private String getMapOutputValue(String[] currRow){
        
        StringBuilder valueString= new StringBuilder();
        float ticketPrice;
        int occurance = 1;
        String month = currRow[2];
        String year = currRow[0];        
        ticketPrice = (currRow[109].isEmpty()) ? 0 : Float.parseFloat(currRow[109]);
        
        valueString.append(year).append(",");
        valueString.append(month).append(",");
        valueString.append(ticketPrice);
                    
        return valueString.toString();
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
                     int month = Integer.parseInt(input[2]);
                     int year = Integer.parseInt(input[0]);
                     float price = Float.parseFloat(input[109]);
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
		if (input[29].isEmpty()||input[40].isEmpty()) {
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
    
    
    // Reducer class 
    public static class FlightTicketReducer extends Reducer<Text, Text, Text, Text> {
        
        private CSVParser csvParser = null;
        static Map<Integer, ArrayList> f_all = new HashMap<Integer, ArrayList>();
        static int countPerMonth[] = new int[12];
        static float f_median[] = new float[12];
        
        //    setup will be called once per Map Task before any of Map function call
        @Override
	protected void setup(Reducer.Context context){
            this.csvParser = new CSVParser(',','"');
        }
        
        
        //Reduce call will be made for every unique key value along with the list of related records
        public void reduce(Text key, Iterable<Text> values,Reducer.Context context) throws IOException, InterruptedException {
            Text reducerVal = new Text();
            Text reducerKey = new Text();
            Arrays.fill(f_median, 0);
            Arrays.fill(countPerMonth, 0);
	    //Arrays.fill(f_avg_price, 0);
            boolean isActive= false;
            ArrayList<Float> tmp = new ArrayList<>();
            for (Text val : values){               
                String[] record = this.csvParser.parseLine(val.toString());
                int year = Integer.parseInt(record[0]);
                int month = Integer.parseInt(record[1]);
                float ticketPrice = Float.parseFloat(record[2]);
                if(f_all.containsKey(Integer.valueOf(month))){
                    tmp = f_all.get(Integer.valueOf(month));
                    tmp.add(ticketPrice);
                    f_all.put(Integer.valueOf(month),tmp);
                }
                else{
                    tmp.add(ticketPrice);
                    f_all.put(Integer.valueOf(month),tmp);
                }                    
                countPerMonth[month-1] += 1;
                // checking if flight is active in 2015
                if(year == 2015)
                    isActive = true;
                
            }
            calculateMedian();
            int totalFlights = calculateTotalFlights();
            
            // preparing output only if the flight is active in 2015
            if (isActive==true){
                StringBuilder reducerKeyString = new StringBuilder();
                for (int i = 0; i<12 ; i++){
                    if(countPerMonth[i] != 0 && f_median[i] != 0 ){
                        reducerKeyString.append(i+1).append("\t");
                        reducerKeyString.append(key.toString());
                    }
                }
                    
                StringBuilder reducerValueString = new StringBuilder();
                for (int i = 0; i<12 ; i++){
                    reducerValueString.append(f_median[i]).append("\t");
                    reducerValueString.append(totalFlights);
                }
                reducerKey.set(reducerKeyString.toString());
                reducerVal.set(reducerValueString.toString());
                context.write(reducerKey, reducerVal);
            }
            
        }
        
        private int calculateTotalFlights(){
            int totalFlights = 0;
            for (int i =0; i<12; i++){
                totalFlights += countPerMonth[i];
            }
            return totalFlights;
        }
        
        /*
        private void calculateAverage (){            
            for (int i =0; i<12; i++){
                if(f_total_price[i]!=0 && countPerMonth[i]!=0)
                    f_avg_price[i] = f_total_price[i]/countPerMonth[i];
                
            }
        }  */
        
        /*
        private void calculateMedian(){
            Set keySet = f_all.keySet();
                Iterator ii = keySet.iterator();
                ArrayList<Float> tmpList;
                while(ii.hasNext()){
                    String key = (String) ii.next();
                    tmpList = f_all.get(key);
                    Collections.sort(tmpList);
                    //f_all.put(key, tmpList);
                    int medianIndx;
                    int month = Integer.parseInt(key);
                    int size = tmpList.size();
                    if(size%2 == 1){
                        medianIndx = size/2;
                        f_median[month-1] = tmpList.get(medianIndx);
                    }
                    else{
                        medianIndx = size/2;
                        float medianVal = (tmpList.get(medianIndx) + tmpList.get(medianIndx-1))/2;
                        f_median[month-1] = medianVal;
                    }
                        
                }        
        }
        */
        
        public void calculateMedian(){
            Set keySet = f_all.keySet();
            Iterator ii = keySet.iterator();
            ArrayList<Float> tmpList;
            while(ii.hasNext()){
                String key = (String) ii.next();
                tmpList = f_all.get(key);
                int month = Integer.parseInt(key);
                int n = f_all.size();
                int k = n/2;
                float medianVal = kthSmallest(tmpList,0,n-1,k);
                f_median[month-1] = medianVal;
            }
        }
        
        public float kthSmallest (ArrayList a, int left, int right ,int k){
                
        if (k> a.size())
            return Float.MAX_VALUE;
        
        // partion position
        int pos = partition(a, left, right);
        if (pos == k-1)
            return (float) a.get(pos);
        
        if(pos>k-1)
            return kthSmallest(a, left, pos-1, k);
        
        else
            return kthSmallest(a, pos+1, right, k);
        
    }
    
    // p = starting of the sub array which has to be partitioned
    // r = last position of the subarray 
    // this function returns the final position of the pivot for the input array arr
    public int partition(ArrayList<Float>  arr, int p , int r){
        
        int finalpivotIndx;
        
        // always consider the last element to be the pivot
        float pivot = arr.get(r);
        // i is the pointer for elelmets which are less than the pivot
        int i = p-1;
        // j is the pointer for the elemts which are more than the pivot
        int j = p;
        // looping j over all the elements which are at positions from p to r-1
        for (j=p;j<=r-1;j++){
            
            // the concept is , at any iteration of this for loop, all the elements from p to current i, will be 
            // smaller than the pivot , and all elemts from i+1 to j will be greater than the pivot 
            if(arr.get(j)<pivot){
                i = i+1;
                Collections.swap(arr, i, j); /*
                float tmp = arr.get(i);
                System.out.println(tmp);
                arr.add(i,arr.get(j));
                System.out.println(arr.get(j));
                arr.add(j,tmp);    */            
            }
        }
        // now we know that the arr[p...r-1] is partitioned around arr[i], ( arr[i] being the last of the smaller elements wrt the pivot
        // so we needa place the pivot at arr[i+1], thus exchanges the places for pivot and arr[i+1]
        finalpivotIndx = i+1;
        
        Collections.swap(arr,r,finalpivotIndx); /*
        float tmp2 = arr.get(finalpivotIndx);
        arr.add(finalpivotIndx,pivot);
        arr.add(r,tmp2);        
        */
        return finalpivotIndx;
    }
        
        
    }
    
    
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Flight Ticket Price");
    job.setJarByClass(Cluster_Analysis_Average.class);
    job.setNumReduceTasks(5);
    job.setMapperClass(FlightTicketMapper.class);
    job.setCombinerClass(FlightTicketReducer.class);
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
