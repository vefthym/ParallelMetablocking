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

public class AfterFilteringBlockSizeByteCounterReducerDirty extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VLongWritable> {
	
	public enum OUTPUT_COUNTER {COMPARISONS};
	
	private VLongWritable zero = new VLongWritable(0);
	private VLongWritable finalSum = new VLongWritable();
	
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VLongWritable> output, Reporter reporter) throws IOException {				
	
		List<VIntWritable> entities = new ArrayList<>();
		
		while (values.hasNext()) {
                    VIntWritable next = values.next();
                    entities.add(next);
		}
		
		if (entities.isEmpty()) {
			output.collect(_key, zero);
			return; //purged block (no comparisons) -> emit 0 comparisons
		}
		
		Collections.sort(entities); //sort entities in ascending order

		//compress the two arrays once
		VIntWritable[] entitiesArray = new VIntWritable[entities.size()];
		VIntArrayWritable allEntities = RelativePositionCompression.compress(entities.toArray(entitiesArray));
		
		long sizeInBytes = 0;
		for (VIntWritable tmp : allEntities.get()) {
			sizeInBytes += WritableUtils.getVIntSize(tmp.get());
		}		
		reporter.incrCounter(OUTPUT_COUNTER.COMPARISONS, (long)(entities.size() * (long)entities.size()-1)/2);
		
		finalSum.set(sizeInBytes*entities.size());
		output.collect(_key, finalSum);
		

	}
	

}
