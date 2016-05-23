
import java.io.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import com.opencsv.CSVReader;
import java.util.logging.Level;
import java.util.logging.Logger;
// Author : Sharmo n Sarita
public class Threaded_Average {
	int c = 0;
	int ss = 0;
        //public static final String directoryWindows ="C:\\Users\\Sharmo\\Documents\\NetBeansProjects\\HW-01\\all";
        // HashMap to store all prices (used for median calculation)
        static Map<String, ArrayList> f_all = new HashMap<String, ArrayList>();
        // For tracking active flight list in 2015 Jan
        static ArrayList<String> active_airlines = new ArrayList<String>();
	public static void main(String args[]) throws IOException, InterruptedException {
            
                //String directoryWindows = args[0];
                String directoryWindows =args[0];
                // HasMap to store the price for each sane airline
		Map<String, Float> f_price = new HashMap<String, Float>();
		// HasMap to store the total occurrence of each sane airline
		Map<String, Integer> f_total = new HashMap<String, Integer>();
		// HasMap to store the mean price of tickets for each sane airline
		Map<String, Float> f_avg_cost = new HashMap<String, Float>();
                // HashMap to store all median values
                Map<String, Float> f_median = new HashMap<String, Float>();
                // Calculate the sane and insane flights
                int K, C;
		K = C = 0;
                // List of Threads
                ArrayList<workerThread> threads = new ArrayList<workerThread>();
                
                System.out.println("Strating up the process . Picking up data files from "+directoryWindows);
                
		Threaded_Average obj = new Threaded_Average();
                //obj.listFilesAndFolders(directoryWindows);
                
                // Reading Files from the Directory                
                File directory = new File(directoryWindows);
                workerThread newThread;
                //get all the files from a directory
                File[] fList = directory.listFiles();
                for (File file : fList){
                    String fName = file.getName();
                    // process .gz files only
                    if (fName.substring(fName.length()-3).compareTo(".gz")==0){
                        //System.out.println(file.getAbsolutePath());
                        // Spawning a new Thread
                        newThread = new workerThread(file.getAbsolutePath(),file.getName(),obj,directoryWindows);
                        // adding a new thread to the list of threads
                        threads.add(newThread);
                        // statating the newly spawned thread
                        newThread.start();
                    }            
                }
                
                for (workerThread th : threads){
                    th.join();
                }
                
                for (workerThread th : threads){
                    //System.out.println("FileNAme :: " + th.fileName + "\t K = "+th.K);
                    K = K+th.K;
                    C = C+th.C;
                    Set keySet = th.f_price.keySet();
                    Iterator ii = keySet.iterator();
                    while (ii.hasNext()){
                        String key = (String) ii.next();
                        if(f_price.containsKey(key)){
                            float value = f_price.get(key);
                            f_price.put(key, th.f_price.get(key)+value);
                        }
                        else
                            f_price.put(key,th.f_price.get(key));                            
                    }
                    
                    keySet =  th.f_total.keySet();
                    ii = keySet.iterator();
                    while(ii.hasNext()){
                        String key = (String) ii.next();
                        if(f_total.containsKey(key)){
                           int val = f_total.get(key);
                           f_total.put(key, th.f_total.get(key)+val);
                        }
                        else
                            f_total.put(key,th.f_total.get(key));
                    }
                    
                }
              //  System.out.println(" K = "+K+"\t C = "+C);                
                
                // THREADS CHUNK END    
                
                // sorting the f_all arraylists for median search 
                /*
                Set keySet = f_all.keySet();
                Iterator ii = keySet.iterator();
                ArrayList<Float> tmpList;
                while(ii.hasNext()){
                    String key = (String) ii.next();
                    tmpList = f_all.get(key);
                    Collections.sort(tmpList);
                    //f_all.put(key, tmpList);
                    int medianIndx;
                    int size = tmpList.size();
                    if(size%2 == 1){
                        medianIndx = size/2;
                        f_median.put(key,tmpList.get(medianIndx));
                    }
                    else{
                        medianIndx = size/2;
                        float medianVal = (tmpList.get(medianIndx) + tmpList.get(medianIndx-1))/2;
                        f_median.put(key,medianVal);
                    }
                        
                }  */
                           
                // computes the mean price of tickets for each carrier and stores in
		// f_avg_cost HashMap
		Set price_list = f_price.keySet();
		Iterator price_iterator = price_list.iterator();
		while (price_iterator.hasNext()) {
			String flight_career = (String) price_iterator.next();
			f_avg_cost.put(flight_career, f_price.get(flight_career) / f_total.get(flight_career));
		}

		// sort the results and display a pair C p
		// output pairs "C p" where C is a carrier two letter code and p is the
		// mean price of tickets
		List<Entry<String, Float>> sortcost = sortTerm(f_avg_cost);
		for (Entry<String, Float> el : sortcost) {
                    if(active_airlines.contains(el.getKey()));
			System.out.println(
					String.format("%1$-" + 20 + "s", el.getKey()) 
                                                + String.format("%1$" + 15 + "s", el.getValue()) );
                                               // + String.format("%1$" + 15 + "s", f_median.get(el.getKey())) );

		}
                
	}
        
        public void addValToF_all(String key, float val){
            ArrayList<Float> tmpList = new ArrayList<>();
            if(f_all.containsKey(key)){
                tmpList = f_all.get(key);
                tmpList.add(val);
                f_all.put(key, tmpList);
            }
            else{
                tmpList.add(val);
                f_all.put(key, tmpList);
                f_all.put(key, tmpList);                        
            }
            
        }
        
        public void addActiveAirlines(String carrier){
            if (!active_airlines.contains(carrier))
                active_airlines.add(carrier);
        } 
        
        // function to sort a HasMap by value
	static <K, V extends Comparable<? super V>> List<Entry<K, V>> sortTerm(Map<K, V> map) {

		List<Entry<K, V>> sortedEntries = new ArrayList<Entry<K, V>>(map.entrySet());

		Collections.sort(sortedEntries, new Comparator<Entry<K, V>>() {
			@Override
			public int compare(Entry<K, V> ob1, Entry<K, V> ob2) {
				return ob1.getValue().compareTo(ob2.getValue());
			}
		});

		return sortedEntries;
	}
        
        // Thread class
        static class workerThread extends Thread{
            // HashMap to store all prices (used for median calculation)
            Map<String, ArrayList> f_all = new HashMap<String, ArrayList>();
            String filePath , fileName ;
            // HasMap to store the price for each sane airline
            Map<String, Float> f_price = new HashMap<String, Float>();
            // HasMap to store the total occurrence of each sane airline
            Map<String, Integer> f_total = new HashMap<String, Integer>();
            // for sane and insane flights
            int K = 0, C = 0;            
            Threaded_Average obj = new Threaded_Average();
            String dir ;
            
            public workerThread(String gzFilePath , String decompressedFileName, Threaded_Average object, String directoryWindows) {
                this.filePath = gzFilePath;
                this.fileName = decompressedFileName;
                this.obj = object;
                this.dir = directoryWindows;
            }
            
            @Override
            public void run(){
                
                try {
                    // path to compressed file
                    String gzip_filepath = filePath;
                    // FileName
                    String decopressed_fileName = fileName.substring(0,fileName.length()-3);
                    // path to uncompressed file
                    String decopressed_filepath = dir+"\\"+decopressed_fileName;
                    // record_corrupt is true if a record doesn't passes the sanity test
                    // else false
                    boolean record_corrupt = false;
                    //int K, C;
                    //K = C = 0;
                    float value = 0;
                    //ArrayList<Float> tmpList = new ArrayList<Float>();
                    Threaded_Average gZipFile = new Threaded_Average();
                    gZipFile.unGunzipFile(gzip_filepath, decopressed_filepath);
                    CSVReader reader = new CSVReader(new FileReader(decopressed_filepath), ',', '"', 1);
                    // Read CSV line by line
                    String[] currRow;
                    while ((currRow = reader.readNext()) != null) {
                        if(currRow.length != 110){
                            K++; 
                            continue;
                        }
                        
                        if (currRow != null) {
                            record_corrupt = gZipFile.sanityTest(currRow);
                            // if record is corrupt, K is incremented else C is incremented
                            // and the valid record are
                            // stored in f_price and f_total HashMap
                            if (record_corrupt) {
                                K = K + 1;
                            } else {                                                              
                                C = C + 1;
                                // BEGIN NEW ADDITION FOR Threaded_Average
                                int year = Integer.parseInt(currRow[0]);
                                int month = Integer.parseInt(currRow[2]);
                                if (year == 2015){
                                    synchronized(obj){
                                        obj.addActiveAirlines(currRow[6]);
                                    }
                                }
                                // END NEW ADDITION FOR Threaded_Average
                                
                                if (currRow[109].isEmpty())
                                    value = 0;
                                else
                                    value = Float.parseFloat(currRow[109]);
                                
                                synchronized(obj){
                                    obj.addValToF_all(currRow[6],value);
                                }
                                
                                if (f_price.containsKey(currRow[6])) {
                                    f_price.put(currRow[6], f_price.get(currRow[6]) + value);
                                    f_total.put(currRow[6], f_total.get(currRow[6]) + 1);
                                    
                                }
                                else {
                                    f_price.put(currRow[6], value);
                                    f_total.put(currRow[6], 1);
                                    
                                }
                            }
                        }
                    }   
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(Threaded_Average.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(Threaded_Average.class.getName()).log(Level.SEVERE, null, ex);
                }
                
            }
            
            
        }
        
        public boolean sanityTest(String[] input) {
		// entry corrupt indicator
		boolean corrupt = false;
                
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
                
                // BEGIN NEW ADDITION FOR Threaded_Average
                boolean yearHasAlpha = input[0].matches(".*[a-zA-Z]+.*");
                boolean monthHasAlpha = input[2].matches(".*[a-zA-Z]+.*");
                // assuming that year and month column shouldnt have any alphabets and can't be empty
                if (yearHasAlpha || monthHasAlpha || input[0].isEmpty() || input[2].isEmpty()){
                    corrupt = true;
                    return corrupt;
                }
                // END OF NEW ADDITION FOR Threaded_Average

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

	// gunzip the file and stores on disk
	public void unGunzipFile(String compressedFile, String decompressedFile) {

		byte[] buffer = new byte[1024];

		try {

			FileInputStream fileIn = new FileInputStream(compressedFile);
			GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
			FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
			int bytes_read;
			while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
				fileOutputStream.write(buffer, 0, bytes_read);
			}
			gZIPInputStream.close();
			fileOutputStream.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
        
        
}
