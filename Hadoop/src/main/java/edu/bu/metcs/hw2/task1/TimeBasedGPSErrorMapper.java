package edu.bu.metcs.hw2.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeBasedGPSErrorMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text hourOfDay = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tripValues = value.toString().split(",");
		
		if(!isDataErroneous(tripValues)) {
			if((tripValues[6]).trim().equals("") || (tripValues[7]).trim().equals("") || 
					(tripValues[8]).trim().equals("") || (tripValues[9]).trim().equals("") ||
					Double.parseDouble(tripValues[6]) == 0 || Double.parseDouble(tripValues[7]) == 0 || 
					Double.parseDouble(tripValues[8]) == 0 || Double.parseDouble(tripValues[9]) == 0) {
				hourOfDay.set(getHourOfDay(tripValues[2]));
				context.write(hourOfDay, new IntWritable(1));
			}
		}
	}
	
	// cleanup real world data
	public boolean isDataErroneous (String[] tripValues) {
		boolean error = false;		
		error = !areAllFieldsPresent(tripValues);		
		return error;
	}
	
	public boolean areAllFieldsPresent(String[] tripValues) {
		return tripValues.length == 17;
	}
	
	public String getHourOfDay(String datetime) {
		String[] dateTimeArray = datetime.split(" ");
		String[] time = dateTimeArray[1].split(":");
		return time[0];
	}
}
