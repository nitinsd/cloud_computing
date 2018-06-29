package edu.bu.metcs.hw2.task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.bu.metcs.hw2.task2.ErrorRateCombinedAndSortMapper;
import edu.bu.metcs.hw2.task2.ErrorRateCombinedAndSortReducer;
import edu.bu.metcs.hw2.task2.WorstNTaxis;

public class BestNDrivers extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BestNDrivers(), args);
        System.exit(res);
	}

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();
            
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job errorRateCalculatorJob = new Job(conf, "BestNDrivers");
            errorRateCalculatorJob.setJarByClass(BestNDrivers.class);

            // specify a Mapper
            errorRateCalculatorJob.setMapperClass(MPMMapper.class);
            
            // specify Mapper key and value class
            errorRateCalculatorJob.setMapOutputKeyClass(Text.class);
            errorRateCalculatorJob.setMapOutputValueClass(DriverMoneyData.class);
            
            // specify a Reducer
            errorRateCalculatorJob.setReducerClass(MPMReducer.class);

            // specify output types
            errorRateCalculatorJob.setOutputKeyClass(Text.class);
            errorRateCalculatorJob.setOutputValueClass(DoubleWritable.class);

            // specify input and output directories  
            FileInputFormat.addInputPath(errorRateCalculatorJob, new Path(args[0]));
            errorRateCalculatorJob.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(errorRateCalculatorJob, new Path(args[1]));
            errorRateCalculatorJob.setOutputFormatClass(TextOutputFormat.class);

            errorRateCalculatorJob.waitForCompletion(true);
            
            // combine and sort
            Configuration conf1 = new Configuration();
            
            conf1.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf1.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job job1 = new Job(conf1, "BestNDrivers");
            job1.setJarByClass(BestNDrivers.class);

            // specify a Mapper
            job1.setMapperClass(MPMCombineAndSortMapper.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(DoubleWritable.class);

            // specify a Reducer
            job1.setReducerClass(MPMCombineAndSortReducer.class);
            
            job1.setNumReduceTasks(1);

            // specify output types
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

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
