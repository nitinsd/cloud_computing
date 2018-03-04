package edu.bu.metcs.hw2.task2;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorRateReducer extends Reducer<Text, TaxiData, Text, FloatWritable> {
	
	private Map<String, Float> taxiErrorRate = new HashMap<>();

	public void reduce(Text text, Iterable<TaxiData> values, Context context) throws IOException, InterruptedException {
		int occurenceCount = 0;
		int errorCount = 0;
		for (TaxiData value : values) {
			occurenceCount += value.getOccurenceCounter();
			errorCount += value.getErrorCounter();
		}
		
		float errorRate = ((float) errorCount/occurenceCount) * 100;

		taxiErrorRate.put(text.toString(), new Float(errorRate));
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, Float> errorRates = taxiErrorRate.entrySet().stream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.limit(5)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		
		for (Map.Entry<String, Float> entry : errorRates.entrySet()) {
		    context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
		}
	}
}