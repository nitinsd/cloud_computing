package edu.bu.metcs.hw2.task2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorRateMapper extends Mapper<Object, Text, Text, TaxiData> {
	private int oneOccurence = 1;
	private int oneError = 1;
	private int noError = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tripValues = value.toString().split(",");

		if(!isTripDataErroneous(tripValues)) {
			Text medallion = new Text (tripValues[0]);
			if(isGpsError(tripValues)) {
				context.write(medallion, new TaxiData (oneOccurence,oneError));
			} else {
				context.write(medallion, new TaxiData (oneOccurence,noError));
			}
		}
	}
	
	// cleanup real world data
	public boolean isTripDataErroneous (String[] tripValues) {
		boolean tripDataError = false;		
		tripDataError = !areAllFieldsPresent(tripValues) || tripValues[0] == null || tripValues[0].trim().equals("");
		return tripDataError;
	}
	
	public boolean areAllFieldsPresent(String[] tripValues) {
		return tripValues.length == 17;
	}
	
	// return true if any of the gps co-ordinates are 0 or missing
	public boolean isGpsError (String[] tripValues) {
		boolean gpsError = false;		
		if(tripValues[6].trim().equals("") || tripValues[7].trim().equals("") || 
				tripValues[8].trim().equals("") || tripValues[9].equals("") ||
				Double.parseDouble(tripValues[6]) == 0 || Double.parseDouble(tripValues[7]) == 0 || 
				Double.parseDouble(tripValues[8]) == 0 || Double.parseDouble(tripValues[9]) == 0) {
			gpsError = true;
		}
		return gpsError;
	}
}
