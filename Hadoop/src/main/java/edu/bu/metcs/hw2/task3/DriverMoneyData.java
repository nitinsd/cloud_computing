package edu.bu.metcs.hw2.task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class DriverMoneyData implements Writable {
	DoubleWritable cumulativeAmount;
	LongWritable cumulativeMinutes;
	
	public DriverMoneyData() {
		cumulativeAmount = new DoubleWritable(0);
		cumulativeMinutes = new LongWritable(0);
	}

	public DriverMoneyData(double cumulativeAmount, long cumulativeMinutes) {
		this.cumulativeAmount = new DoubleWritable(cumulativeAmount);
		this.cumulativeMinutes = new LongWritable(cumulativeMinutes);
	}

	public double getCumulativeAmount() {
		return cumulativeAmount.get();
	}

	public long getCumulativeMinutes() {
		return cumulativeMinutes.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cumulativeAmount.readFields(in);
		cumulativeMinutes.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		cumulativeAmount.write(out);
		cumulativeMinutes.write(out);
	}
}
