package edu.bu.metcs.HadoopEx;

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

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
	}

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();
            
//            Different JARs (hadoop-commons for LocalFileSystem, hadoop-hdfs for DistributedFileSystem) each contain a different file
//            called org.apache.hadoop.fs.FileSystem in their META-INFO/services directory. This file lists the canonical classnames 
//            of the filesystem implementations they want to declare (This is called a Service Provider Interface implemented via 
//            java.util.ServiceLoader, see org.apache.hadoop.FileSystem line 2622).
//            When we use maven-assembly-plugin, it merges all our JARs into one, and all META-INFO/services/org.apache.hadoop.fs.FileSystem
//            overwrite each-other. Only one of these files remains (the last one that was added). In this case, the FileSystem list from 
//            hadoop-commons overwrites the list from hadoop-hdfs, so DistributedFileSystem was no longer declared.
//            This is how we fix it
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Job job = new Job(conf, "WordCount");
            job.setJarByClass(WordCount.class);

            // specify a Mapper
            job.setMapperClass(WordCountMapper.class);

            // specify a Reducer
            job.setReducerClass(WordCountReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output directories  
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            return(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
        	System.out.println(new Path(args[0]));
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}
