/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import hadoopUtils.RelativePositionCompression;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedReducerCEPARCSDirty extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, DoubleWritable, VIntWritable> {
	
	VIntWritable one = new VIntWritable(1);
	DoubleWritable weightToEmit = new DoubleWritable();

	/**
	 * @param _key an entity id
	 * @param values the list of arrays with entity ids appearing in a block with the _key entity
	 * 
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<DoubleWritable, VIntWritable> output, Reporter reporter) throws IOException {		
		long bComparisons;
		Map<Integer, Double> weights = new HashMap<>(); //key: neighborId, value: ARCS weightweights = new HashMap<>();
		while (values.hasNext()) {
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next());
			//in dirty ER ||b|| = (|b| * |b|-1) /2
			bComparisons = ((next.length) * (next.length-1)) / 2; //cannot be zero, cannot be odd number (no casting needed)
			for (VIntWritable neighborId : next) { 
				if (neighborId.equals(_key)) {
					continue;
				}
				int neighbor = neighborId.get();

				Double prevWeight = weights.get(neighbor);
				if (prevWeight == null) {
					prevWeight = 0.0;
				}				
				weights.put(neighbor, prevWeight + 1.0/bComparisons);
            }
		}

		for (double weight : weights.values()) { //iterate over the weights only (ignore labels)
			weightToEmit.set(weight);			
			output.collect(weightToEmit, one);			
		}
	
	
	}

}
