package edu.bu.metcs.hw2.task2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WorstNTaxis extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WorstNTaxis(), args);
        System.exit(res);
	}

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();
            
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job errorRateJob = new Job(conf, "ErrorRate");
            errorRateJob.setJarByClass(WorstNTaxis.class);

            // specify a Mapper
            errorRateJob.setMapperClass(ErrorRateMapper.class);
            
            // specify Mapper key and value class
            errorRateJob.setMapOutputKeyClass(Text.class);
            errorRateJob.setMapOutputValueClass(TaxiData.class);
            
            // specify a Reducer
            errorRateJob.setReducerClass(ErrorRateReducer.class);
            errorRateJob.setNumReduceTasks(3);

            // specify output types
            errorRateJob.setOutputKeyClass(Text.class);
            errorRateJob.setOutputValueClass(FloatWritable.class);

            // specify input and output directories  
            FileInputFormat.addInputPath(errorRateJob, new Path(args[0]));
            errorRateJob.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(errorRateJob, new Path(args[1]));
            errorRateJob.setOutputFormatClass(TextOutputFormat.class);

            errorRateJob.waitForCompletion(true);
            
            // combine and sort
            Configuration conf1 = new Configuration();
            
            conf1.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf1.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job job1 = new Job(conf1, "WorstNTaxis");
            job1.setJarByClass(WorstNTaxis.class);

            // specify a Mapper
            job1.setMapperClass(ErrorRateCombinedAndSortMapper.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(FloatWritable.class);

            // specify a Reducer
            job1.setReducerClass(ErrorRateCombinedAndSortReducer.class);
            
            job1.setNumReduceTasks(1);

            // specify output types
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(FloatWritable.class);

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
