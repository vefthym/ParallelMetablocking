/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import hadoopUtils.RelativePositionCompression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AfterFilteringBlockSizeByteCounterReducer extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VLongWritable> {
	
	public enum OUTPUT_COUNTER {COMPARISONS};
	
	private VLongWritable zero = new VLongWritable(0);
	private VLongWritable finalSum = new VLongWritable();
	
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VLongWritable> output, Reporter reporter) throws IOException {				
	
		//separate positives from negatives (for clean-clean)
		List<VIntWritable> positives = new ArrayList<>();
		List<VIntWritable> negatives = new ArrayList<>();
		
		while (values.hasNext()) {
			VIntWritable next = values.next();
			if (next.get() < 0) {
				negatives.add(next);
			} else {
				positives.add(next);
			}
		}
		
		if (positives.isEmpty() || negatives.isEmpty()) {
			output.collect(_key, zero);
			return; //purged block (no comparisons) -> emit 0 comparisons
		}
		
		Collections.sort(positives); //sort positives in ascending order
		Collections.sort(negatives, Collections.reverseOrder()); //sort negatives in descending order (saves more space in compression)

		//compress the two arrays once
		VIntWritable[] positivesArray = new VIntWritable[positives.size()];
		VIntWritable[] negativesArray = new VIntWritable[negatives.size()];		
		VIntArrayWritable positiveEntities = RelativePositionCompression.compress(positives.toArray(positivesArray));
		VIntArrayWritable negativeEntities = RelativePositionCompression.compress(negatives.toArray(negativesArray));
		
		long positivesSizeInBytes = 0;
		for (VIntWritable tmp : positiveEntities.get()) {
			positivesSizeInBytes += WritableUtils.getVIntSize(tmp.get());
		}		
		positivesSizeInBytes *= negatives.size();
		
		
		long negativesSizeInBytes = 0;
		for (VIntWritable tmp : negativeEntities.get()) {
			negativesSizeInBytes += WritableUtils.getVIntSize(tmp.get());
		}		
		negativesSizeInBytes *= positives.size();
		
		reporter.incrCounter(OUTPUT_COUNTER.COMPARISONS, (long) positives.size() * negatives.size());
		
		finalSum.set(positivesSizeInBytes+negativesSizeInBytes);
		output.collect(_key, finalSum);
		

	}
	

}
