package edu.bu.metcs.hw2.task2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorRateCombinedAndSortMapper extends Mapper<Object, Text, Text, FloatWritable> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value != null && !value.equals("")) {
			String[] str = value.toString().split("\\s+");
			if(str[0] != null && !str[0].trim().equals("") && str[1] != null && !str[1].trim().equals("")) {
				context.write(new Text(str[0]), new FloatWritable(Float.parseFloat(str[1])));
			}
		}
	}
}