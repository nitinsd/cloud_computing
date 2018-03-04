package edu.bu.metcs.hw2.task3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MPMMapper extends Mapper<Object, Text, Text, DriverMoneyData> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tripValues = value.toString().split(",");

		if(!isTripDataErroneous(tripValues)) {
			Text driver = new Text (tripValues[1]);
			double amount = 0;
			if(!tripValues[16].trim().equals(""))
				amount = Double.parseDouble(tripValues[16]);
			context.write(driver, new DriverMoneyData (amount,calculateTimeForTrip(tripValues[2], tripValues[3])));
		}
	}
	
	// is there any problem with data to calculate MPM? 
	// other irrelevant problems like GPS errors are ignored because 
	// we want to calculate with as much data as possible
	public boolean isTripDataErroneous (String[] tripValues) {
		boolean tripDataError = false;		
		tripDataError = !areAllFieldsPresent(tripValues) || !isValidDriver(tripValues) || !areTimesValid(tripValues);
		return tripDataError;
	}
	
	// are all fields present?
	public boolean areAllFieldsPresent(String[] tripValues) {
		return tripValues.length == 17;
	}
	
	// is driver valid?
	public boolean isValidDriver(String[] tripValues) {
		return !(tripValues[1] == null || tripValues[1].trim().equals(""));
	}
	
	// are pickup and drop off times valid?
	public boolean areTimesValid(String[] tripValues) {
		return !(tripValues[2] == null || tripValues[2].trim().equals("") || tripValues[3] == null || tripValues[3].trim().equals(""));
	}

	// how long was the trip in minutes?
	public long calculateTimeForTrip(String pickup, String dropoff) {
		long minutes = 0;
		
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date pickupDate = format.parse(pickup);
			Date dropoffDate = format.parse(dropoff);
			long diff = dropoffDate.getTime() - pickupDate.getTime();
			minutes = diff / (60 * 1000) % 60;
		} catch (Exception e) {
			
		}
		return minutes;
	}
}
