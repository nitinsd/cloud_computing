package edu.bu.metcs.hw2.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorCombinedAndSortMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value != null && !value.equals("")) {
			String[] str = value.toString().split("\\s+");
			if(str[0] != null && !str[0].trim().equals("") && str[1] != null && !str[1].trim().equals("")) {
				context.write(new IntWritable(Integer.parseInt(str[1])), new Text(str[0]));
			}
		}
	}
}
