package hw.pkg01;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class HW01 {

    private static final String INPUT_GZIP_FILE = "323.csv.gz";
    private static final String DECOMPRESSED_FILE = "test.csv";
    private  HashMap<String, Double> avgPrice = new HashMap<String, Double>();
    private HashMap<String, Integer> numberOfOccurance = new HashMap<String, Integer>();
    private HashMap<String, Double> finalAvgCost = new HashMap<String, Double>();
    
    public static void main( String[] args ) throws IOException
    {
    	HW01 obj = new HW01();
        obj.decompresFile();
        obj.readFile();
    }
    
    /* NOTE ::
        currRow[40] -- CRS_ARR_TIME
        currRow[29] -- CRS_DEP_TIME
    */
    
    public void readFile() throws FileNotFoundException, IOException{
        CSVReader reader = new CSVReader(new FileReader(DECOMPRESSED_FILE),',','"',1);
        String [] currRow;
        long K=0;
        long F=0;
        int i =0;
        while ((currRow = reader.readNext()) != null ){//&& i==0) {
            //System.out.println(currRow[40]);
            //System.out.println(currRow[50]); i++;
            String CRSArrTime = currRow[40];
            String CRSDepTime  = currRow[29];
            if (sanityCheck(currRow)){
                F++;
                trackPrice(currRow);
            }
            else
                K++;
                    
        }
        
        System.out.println("K= "+K);
        System.out.println("F= "+F);
        /*
        for (Map.Entry<String, Double> entry : avgPrice.entrySet()) {
		System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	}
        */
        Set price_list = avgPrice.keySet();
        Iterator ii = price_list.iterator();
	while (ii.hasNext()) {
            String career = (String) ii.next();
            finalAvgCost.put(career, avgPrice.get(career) / numberOfOccurance.get(career));
	}
        
        Map sortedMap = sortByValue(finalAvgCost);
        System.out.println("The Folllowing are the average cost of all the flight carriers");
        System.out.println(sortedMap);
        //System.out.println("On an average));
        
        /*
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
		System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	}*/
    }
    
    public static Map sortByValue(Map unsortedMap) {
		Map sortedMap = new TreeMap(new ValueComparator(unsortedMap));
		sortedMap.putAll(unsortedMap);
		return sortedMap;
	}
    
    public void trackPrice(String [] currRow){
        double avgTicketPrice=0.0;
        if(currRow[109].isEmpty())
            avgTicketPrice = 0.0;
        else
            avgTicketPrice = Double.parseDouble(currRow[109]) ;
        
        if(avgPrice.containsKey(currRow[6])){
            numberOfOccurance.put(currRow[6],numberOfOccurance.get(currRow[6])+1);
            //float avgTicketPrice = Float.parseFloat(currRow[109]) ;
            double price = avgPrice.get(currRow[6]) + avgTicketPrice;
            avgPrice.put(currRow[6], price);
        }
        else{
            numberOfOccurance.put(currRow[6],1);
            //System.out.println(avgTicketPrice);
            //System.out.println(currRow[6]);
            avgPrice.put(currRow[6], avgTicketPrice);
        }
        
    }
    
    public boolean sanityCheck(String [] currRow){        
        
        try {
               int CRSArrTime = 0; 
               int CRSDepTime = 0;
               int  CRSElapsedTime =0;
               int  timeZone=0;
               int o_AirportID,  o_AirportSeqID, o_CityMarketID, o_StateFips, o_Wac;
               int d_AirportID,  d_AirportSeqID, d_CityMarketID, d_StateFips, d_Wac,ActualElapsedTime;
               String origin,   o_CityName, o_State, o_StateName;
               String  destination,   d_CityName, d_State, d_StateName;
               int CancelledFlightInd = Integer.parseInt(currRow[47]);
               if(currRow[40].length() == 4 && currRow[29].length()==4){
               //System.out.println(currRow[40].length() + ":::::" + currRow[29].length());
                    CRSArrTime = Integer.parseInt(currRow[40].substring(0,2))*60 + Integer.parseInt(currRow[40].substring(2,4));
                    CRSDepTime = Integer.parseInt(currRow[29].substring(0,2))*60 + Integer.parseInt(currRow[29].substring(2,4));
               }else
                   return false;
               CRSElapsedTime = Integer.parseInt(currRow[50]);
               o_AirportID =  Integer.parseInt(currRow[11]);
               ActualElapsedTime = Integer.parseInt(currRow[50]);
               CancelledFlightInd = Integer.parseInt(currRow[47]);
               o_AirportSeqID = Integer.parseInt(currRow[12]);
               o_CityMarketID = Integer.parseInt(currRow[13]);
               o_StateFips = Integer.parseInt(currRow[17]);
               o_Wac = Integer.parseInt(currRow[19]);
               d_AirportID = Integer.parseInt(currRow[20]);
               d_AirportSeqID = Integer.parseInt(currRow[21]);
               d_CityMarketID = Integer.parseInt(currRow[22]);
               d_StateFips = Integer.parseInt(currRow[26]);
               d_Wac = Integer.parseInt(currRow[28]);
               origin = currRow[14];
               o_CityName = currRow[15];
               o_State = currRow[16];
               o_StateName = currRow[18];
               destination  = currRow[23];
               d_CityName = currRow[24];
               d_State = currRow[25];
               d_StateName = currRow[27];
               
                       
               timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
               //System.out.println(timeZone);
               if (CRSArrTime!=0 && CRSDepTime!=0 && timeZone%60==0 &&
                    o_AirportID 	>	0	&&
                    o_AirportSeqID 	>	0	&&
                    o_CityMarketID 	>	0	&&
                    o_StateFips 	>	0	&&
                    o_Wac               >	0	&&
                    d_AirportID 	>	0	&&
                    d_AirportSeqID 	>	0	&&
                    d_CityMarketID 	>	0	&&
                    d_StateFips 	>	0	&&
                    d_Wac               >	0       &&
                           origin.length()>0	&&
                           o_CityName.length()>0	&&
                           o_State.length()>0	&&
                           o_StateName.length()>0	&&
                           destination.length()>0	&&
                           d_CityName.length()>0	&&
                           d_State.length()>0	&&
                           d_StateName.length()>0	
                       ){
                   return true;
                           }
               else
                    return false; 
        } catch (NumberFormatException e) {
                return false;
        }
        /*
        finally{      
            if (CRSArrTime==0 || CRSDepTime==0 )//|| timeZone%60 ==0 )
                return false;
            else
                return true;  
        }*/
    }
    
    
    public void decompresFile(){ 
     byte[] buffer = new byte[10240]; 
     try{
            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(INPUT_GZIP_FILE));
            FileOutputStream out = new FileOutputStream(DECOMPRESSED_FILE); 
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
            }
            gzis.close();
            out.close();
            System.out.println("Done");
    	
    }catch(IOException ex){
       ex.printStackTrace();   
    }
   } 
}


 
class ValueComparator implements Comparator {
 
	Map map;
 
	public ValueComparator(Map map) {
		this.map = map;
	}
 
	public int compare(Object keyA, Object keyB) {
		Comparable valueA = (Comparable) map.get(keyA);
		Comparable valueB = (Comparable) map.get(keyB);
		return valueA.compareTo(valueB);
	}
}