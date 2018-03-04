package edu.bu.metcs.HadoopEx;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

public class MapReduceTest {

	public static class Map extends Mapper<LongWritable, Text, LongWritable, TupleWritable> {

		private final static LongWritable id = new LongWritable();
		private final static LongWritable start = new LongWritable();
		private final static LongWritable stop = new LongWritable();
		
		private LongWritable[] tupleValues = new LongWritable[2];
		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			id.set(Long.parseLong(tokenizer.nextToken()));
			start.set(Long.parseLong(tokenizer.nextToken()));
			stop.set(Long.parseLong(tokenizer.nextToken()));
			
			tupleValues[0] = start;
			tupleValues[1] = stop;

			TupleWritable tuple = new TupleWritable(tupleValues);
		
			
System.out.println("TUPLE  --  " + tuple.get(0));
			context.write(id, tuple);
		}
	}

	public static class Reduce extends Reducer<LongWritable, TupleWritable, LongWritable, IntWritable> {

		private final static Text word = new Text("prova");
		private final static IntWritable one = new IntWritable(1);

		public void reduce(LongWritable key, Iterable<TupleWritable> values, Context context) throws IOException,
				InterruptedException {
	for (TupleWritable tupleWritable : values) {
		LongWritable mynumber0=(LongWritable)tupleWritable.get(0);
		LongWritable mynumber1=(LongWritable)tupleWritable.get(1);

		System.out.println(mynumber0 + " - " + mynumber1);
	}
			context.write(key, one);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");

		job.setJarByClass(WordCount.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(TupleWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}