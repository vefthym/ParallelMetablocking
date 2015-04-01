/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CNP extends MapReduceBase implements Reducer<VIntWritable, Text, Text, DoubleWritable> {
	
	public enum Output {NUM_RECORDS};
	
	private int k; //for topK
	public void configure (JobConf job) {
		float BCin = job.getFloat("BCin", 1.0f);
		k = ((Double)Math.floor(BCin - 1)).intValue();
	}
		
	/**	 
	 * output for each input node its edges with weight in top k weights
	 * @param key i entity id
	 * @param value list of j,wij (entity id, weight of edge i-j)
	 * @param output key:i,j value:wij for wij in top k weights
	 */
	public void reduce(VIntWritable key, Iterator<Text> values,
		OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {				
			
		//sort neighbors in descending order of weight (key=weigh, value=neighborID)
		Map<Double,Integer> neighbors = new TreeMap<>(Collections.reverseOrder());	
		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			Double weight = Double.parseDouble(value[1]);
			Integer neighbor = Integer.parseInt(value[0]);
			neighbors.put(weight, neighbor);
		}			
		
		//Emit top k edges (k nearest neighbors)
		for (Map.Entry<Double, Integer> edge : neighbors.entrySet()) {
			if (k-- == 0) { return; }
			if (key.get() >= 0)  { //to make sure they will go to the same reducer (reciprocal)
				reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space
				//output.collect(new Text(key+","+edge.getValue()), new DoubleWritable(edge.getKey()));
			} else {
				reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space
				//output.collect(new Text(edge.getValue()+","+key), new DoubleWritable(edge.getKey()));
			}			
		}
			
	}

}
