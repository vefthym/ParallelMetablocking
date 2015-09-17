/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import hadoopUtils.RelativePositionCompression;
import hadoopUtils.ValueComparator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedReducerCNPARCSClean extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	VIntWritable neighborToEmit = new VIntWritable();
	
	public enum Output {NUM_RECORDS};
	
	private int k; //for topK
	
	public void configure (JobConf conf) {		
		float BCin = conf.getFloat("BCin", 1.0f);
		k = ((Double)Math.floor(BCin - 1)).intValue();		
	}

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
			VIntWritable[] next = RelativePositionCompression.uncompressFromSecond(values.next()); 
			//in clean-clean ER ||b|| = positives * negatives
			bComparisons = (next.length-1) * next[0].get(); //next.length-1, because next[0] is the number of entities in the other collection, not an entity id 
			
			for (int i = 1; i < next.length; i++) {
				VIntWritable neighborId = next[i];			 
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
		
		ValueComparator vc = new ValueComparator(weights); //sorts by value (descending)
		TreeMap<Integer, Double> sortedMap = new TreeMap<>(vc); 
		sortedMap.putAll(weights);
		
		for(Integer neighborId : sortedMap.keySet()) {
			if (k-- == 0) { //continue until k values have been emitted
				return;
			} 
			neighborToEmit.set(neighborId);
//			output.collect(_key, neighborToEmit);  //skip writing the actual output
			reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space and time
		}
	
	
	
	
	}

}
