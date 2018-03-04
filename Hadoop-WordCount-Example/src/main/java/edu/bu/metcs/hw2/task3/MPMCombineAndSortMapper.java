package edu.bu.metcs.hw2.task3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MPMCombineAndSortMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value != null && !value.equals("")) {
			String[] str = value.toString().split("\\s+");
			if(str[0] != null && !str[0].trim().equals("") && str[1] != null && !str[1].trim().equals("")) {
				context.write(new Text(str[0]), new DoubleWritable(Float.parseFloat(str[1])));
			}
		}
	}
}