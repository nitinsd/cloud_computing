package edu.bu.metcs.hw2.task2;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorRateCombinedAndSortReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	PriorityQueue<MedallionErrorRateTuple> queue = new PriorityQueue<MedallionErrorRateTuple>(5, new ErrorRateAscendingComparator());

	public void reduce(Text medallion, Iterable<FloatWritable> errorRates, Context context) throws IOException, InterruptedException {
		for (FloatWritable errorRate : errorRates) {
			MedallionErrorRateTuple t = new MedallionErrorRateTuple(medallion.toString(), errorRate.get());
			if(queue.size() == 5) {
				if(errorRate.get() > queue.peek().getErrorRate()) {
					queue.poll();
					queue.add(t);
				}
			} else {
				queue.add(t);
			}
		}
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		PriorityQueue<MedallionErrorRateTuple> reverseQueue = new PriorityQueue<MedallionErrorRateTuple>(5, new ErrorRateDescendingComparator());
		while(!queue.isEmpty()){
			reverseQueue.add(queue.poll());
		}
		while(!reverseQueue.isEmpty()){
			MedallionErrorRateTuple t = reverseQueue.poll();
			if(t != null) {
				context.write(new Text(t.getMedallion()), new FloatWritable(t.getErrorRate()));
			}
		}
	}
}