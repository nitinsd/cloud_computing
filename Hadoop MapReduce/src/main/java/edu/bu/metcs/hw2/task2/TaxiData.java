package edu.bu.metcs.hw2.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TaxiData implements Writable {
	IntWritable occurenceCounter;
	IntWritable errorCounter;
	
	public TaxiData() {
		occurenceCounter = new IntWritable(0);
		errorCounter = new IntWritable(0);
	}

	public TaxiData(int occurenceCounter, int errorCounter) {
		this.occurenceCounter = new IntWritable(occurenceCounter);
		this.errorCounter = new IntWritable(errorCounter);
	}

	public int getOccurenceCounter() {
		return occurenceCounter.get();
	}

	public int getErrorCounter() {
		return errorCounter.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		occurenceCounter.readFields(in);
		errorCounter.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		occurenceCounter.write(out);
		errorCounter.write(out);
	}
}
