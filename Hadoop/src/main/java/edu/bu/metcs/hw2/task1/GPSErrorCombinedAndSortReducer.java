package edu.bu.metcs.hw2.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GPSErrorCombinedAndSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value, key);
		}
    }
}