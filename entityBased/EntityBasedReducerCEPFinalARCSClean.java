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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


import preprocessing.VIntArrayWritable;

public class EntityBasedReducerCEPFinalARCSClean extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	public enum Output {NUM_RECORDS};
	
	VIntWritable neighborToEmit = new VIntWritable();	
	
	double minWeight;
	
	public void configure (JobConf conf) {
		minWeight = Double.parseDouble(conf.get("min", "0.0"));		
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
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next());
			//in clean-clean ER ||b|| = positives * negatives
			bComparisons = (next.length-1) * next[0].get(); //next.length-1, because next[0] is the number of entities in the other collection, not an entity id
			
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
		
		//prune the neighbors with weight below minWeight
		for (int neighborId : weights.keySet()) {
			double currentWeight = weights.get(neighborId);
			
			if (currentWeight >= minWeight) {
				neighborToEmit.set(neighborId);
//				output.collect(key, neighborToEmit); //comment out to save disk space
				reporter.incrCounter(Output.NUM_RECORDS, 1); //to save disk space (instead of command above)
			}
		}
	
	
	}

}
