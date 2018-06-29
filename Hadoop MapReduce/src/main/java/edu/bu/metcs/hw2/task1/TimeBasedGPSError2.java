package edu.bu.metcs.hw2.task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TimeBasedGPSError2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TimeBasedGPSError2(), args);
        System.exit(res);
	}

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();
            
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job job = new Job(conf, "TimeBasedGPSError2");
            job.setJarByClass(TimeBasedGPSError2.class);

            // specify a Mapper
            job.setMapperClass(TimeBasedGPSErrorMapper.class);

            // specify a Reducer
            job.setReducerClass(TimeBasedGPSErrorReducer.class);
            
            job.setNumReduceTasks(1);


            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output directories  
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
            
            // combine and sort
            Configuration conf1 = new Configuration();
            
            conf1.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf1.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job job1 = new Job(conf1, "CombineAndSort");
            job1.setJarByClass(TimeBasedGPSError2.class);

            // specify a Mapper
            job1.setMapperClass(GPSErrorCombinedAndSortMapper.class);

            // specify a Reducer
            job1.setReducerClass(GPSErrorCombinedAndSortReducer.class);
            
            job1.setNumReduceTasks(1);

            // specify output types
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Text.class);
            job1.setSortComparatorClass(SortIntegerComparator.class);
            
            // specify input and output directories 
            FileInputFormat.addInputPath(job1, new Path(args[1]));
            job1.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            job1.setOutputFormatClass(TextOutputFormat.class);

            return(job1.waitForCompletion(true) ? 0 : 1);

        } catch (InterruptedException|ClassNotFoundException|IOException e) {
        	System.out.println(new Path(args[0]));
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }   
}
