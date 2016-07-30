/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import hadoopUtils.RelativePositionCompression;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import preprocessing.VIntArrayWritable;

public class EntityBasedReducerWNPARCSDirty extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	VIntWritable neighborToEmit = new VIntWritable();
	
	public enum Output {NUM_RECORDS};

	/**
	 * @param _key an entity id
	 * @param values the list of arrays with entity ids appearing in a block with the _key entity
	 * @param output the input with the values deduplicated (i.e., each entity appearing only once)
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {		
				long bComparisons;
		Map<Integer, Double> weights = new HashMap<>(); //key: neighborId, value: ARCS weightweights = new HashMap<>();		
		while (values.hasNext()) {
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next()); 
			//in dirty ER ||b|| = (|b| * |b|-1) /2
			bComparisons = ((next.length) * (next.length-1)) / 2; //cannot be zero			
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
		
		double averageWeight = 0;
		
		//first loop to find the average weight
		for (int neighborId : weights.keySet()) {							
			averageWeight += weights.get(neighborId);
		}
		averageWeight /= weights.keySet().size(); //no of comparisons
		
		//second loop to emit weights above average
		for (int neighborId : weights.keySet()) {
			if (averageWeight < weights.get(neighborId)) {
				neighborToEmit.set(neighborId);
//				output.collect(_key, neighborToEmit);  //skip writing the actual output
				reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space
			}
		}
	
	
	
	
	}

}
