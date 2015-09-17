/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import hadoopUtils.RelativePositionCompression;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import preprocessing.VIntArrayWritable;

public class NodeDegreeReducer extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	VIntWritable numNeighbors = new VIntWritable();
	
	public enum Output {NUM_COMPARISONS};

	/**
	 * @param _key an entity id
	 * @param values the list of arrays with entity ids appearing in a block with the _key entity
	 * @param output number of unique comparisons for the input entity id _key
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {		

		Set<VIntWritable> neighbors = new HashSet<>(); 		
		while (values.hasNext()) {
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next());			
			neighbors.addAll(Arrays.asList(next)); //also adds itself in dirty ER
		}	
		
		//dirty ER
//		numNeighbors.set(neighbors.size()-1); //dirty ER (-1 because neighbors contains the input key)
//		reporter.incrCounter(Output.NUM_COMPARISONS, neighbors.size()-1); //dirty ER

		//clean-clean ER
		numNeighbors.set(neighbors.size()); //clean-clean ER
		reporter.incrCounter(Output.NUM_COMPARISONS, neighbors.size()); //clean-clean ER
		
		
		output.collect(_key, numNeighbors);
	}

}
