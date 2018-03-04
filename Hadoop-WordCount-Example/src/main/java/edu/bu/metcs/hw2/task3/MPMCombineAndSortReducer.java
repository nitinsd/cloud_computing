package edu.bu.metcs.hw2.task3;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MPMCombineAndSortReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	PriorityQueue<DriverMPMTuple> queue = new PriorityQueue<DriverMPMTuple>(5, new MPMAscendingComparator());

	public void reduce(Text driver, Iterable<DoubleWritable> mpms, Context context) throws IOException, InterruptedException {
		for (DoubleWritable mpm : mpms) {
			DriverMPMTuple t = new DriverMPMTuple(driver.toString(), mpm.get());
			if(queue.size() == 5) {
				if(mpm.get() > queue.peek().getMpm()) {
					queue.poll();
					queue.add(t);
				}
			} else {
				queue.add(t);
			}
		}
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		PriorityQueue<DriverMPMTuple> reverseQueue = new PriorityQueue<DriverMPMTuple>(5, new MPMDescendingComparator());
		while(!queue.isEmpty()){
			reverseQueue.add(queue.poll());
		}
		while(!reverseQueue.isEmpty()){
			DriverMPMTuple t = reverseQueue.poll();
			if(t != null) {
				context.write(new Text(t.getDriver()), new DoubleWritable(t.getMpm()));
			}
		}
	}
}