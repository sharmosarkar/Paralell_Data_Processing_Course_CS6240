package hw01;

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

public class DataProcessor {
	int c = 0;
	int ss = 0;

	public static void main(String ar[]) throws IOException {

		// HasMap to store the price for each sane airline
		Map<String, Float> f_price = new HashMap<String, Float>();
		// HasMap to store the total occurrence of each sane airline
		Map<String, Double> f_total = new HashMap<String, Double>();
		// HasMap to store the mean price of tickets for each sane airline
		Map<String, Double> f_avg_cost = new HashMap<String, Double>();

		// path to compressed file
		String gzip_filepath = ar[0];
		// path to uncompressed file
		String decopressed_filepath = "323.csv";
		// record_corrupt is true if a record doesn't passes the sanity test
		// else false
		boolean record_corrupt = false;

		// K is the number of corrupt lines of input
		// (lines not in the same format as the rest and lines with flights that
		// do not pass the sanity test)
		// F is the number of sane flights.
		int K, C;
		K = C = 0;
		float value = 0;

		DataProcessor gZipFile = new DataProcessor();

		gZipFile.unGunzipFile(gzip_filepath, decopressed_filepath);

		CSVReader reader = new CSVReader(new FileReader("323.csv"), ',', '"', 1);

		// Read CSV line by line
		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			if (nextLine != null) {
				record_corrupt = gZipFile.sanityTest(nextLine);
				// if record is corrupt, K is incremented else C is incremented
				// and the valid record are
				// stored in f_price and f_total HashMap
				if (record_corrupt) {
					K = K + 1;
				} else {

					C = C + 1;
					if (nextLine[109].isEmpty())
						value = 0;
					else
						value = Float.parseFloat(nextLine[109]);

					if (f_price.containsKey(nextLine[6])) {

						f_price.put(nextLine[6], f_price.get(nextLine[6]) + value);
						f_total.put(nextLine[6], f_total.get(nextLine[6]) + 1);
					} else {
						f_price.put(nextLine[6], value);
						f_total.put(nextLine[6], 1.0);
					}
				}
			}
		}
		System.out.println("K is " + K);
		System.out.println("C is " + C);

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
		List<Entry<String, Double>> sortcost = sortTerm(f_avg_cost);
		for (Entry<String, Double> el : sortcost) {
			System.out.println(
					String.format("%1$-" + 20 + "s", el.getKey()) + String.format("%1$" + 15 + "s", el.getValue()));

		}

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

		// check if CRS_DEP_TIME contains characters
		// if it contains, then record is marked as corrupt
		if (atleastOneAlpha) {
			corrupt = true;
			return corrupt;

		}

		// check if CRS_DEP_TIME is empty
		// if it is empty, then record is marked as corrupt
		if (input[29].isEmpty()) {
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

		} else {

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

		if (input[30].isEmpty()) {
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

		} else {
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
			System.out.println(ss);
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
