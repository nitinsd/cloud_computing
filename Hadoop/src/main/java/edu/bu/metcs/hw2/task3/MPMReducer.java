package edu.bu.metcs.hw2.task3;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MPMReducer extends Reducer<Text, DriverMoneyData, Text, DoubleWritable> {
	
	private Map<String, Double> driverMPM = new HashMap<>();

	public void reduce(Text text, Iterable<DriverMoneyData> values, Context context) throws IOException, InterruptedException {
		double cumulativeAmount = 0;
		long cumulativeMinutes = 0;
		for (DriverMoneyData value : values) {
			cumulativeAmount += value.getCumulativeAmount();
			cumulativeMinutes += value.getCumulativeMinutes();
		}
		
		if(cumulativeMinutes != 0) {
			double mpm = (double) cumulativeAmount/cumulativeMinutes;
			driverMPM.put(text.toString(), new Double(mpm));
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, Double> errorRates = driverMPM.entrySet().stream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.limit(10)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		
		for (Map.Entry<String, Double> entry : errorRates.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		    context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
		}
	}
}