/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EntityIndexReducerNoFiltering extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> {
	
	
	static enum OutputData {D1Entities, D2Entities, BLOCK_ASSIGNMENTS}; 
		
	/**
	 * Builds the Entity Index, after performing Block Filtering
	 * To skip the block filtering part, just output all the blocks and not the top MAX_BLOCKS
	 * by commenting out the specified line
	 * input _key: entity id
	 * input values: block ids of the current entity 
	 */
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
			
		Set<VIntWritable> toEmit = new TreeSet<>(); //to sort the blocks in ascending order
		while (values.hasNext()) {
			Integer block = values.next().get(); //the block id 		
			toEmit.add(new VIntWritable(block));			
		} 
		
/*		//transform the set to an array, which will be the final output (toEmitFinal)
		VIntWritable[] toEmitArray = new VIntWritable[toEmit.size()];
		toEmitArray = toEmit.toArray(toEmitArray);		
		VIntArrayWritable toEmitFinal = new VIntArrayWritable(toEmitArray);*/	
		
		VIntArrayWritable toEmitFinal = hadoopUtils.RelativePositionCompression.compress(toEmit);
		
		if (_key.get() >= 0) {
			reporter.incrCounter(OutputData.D1Entities, 1);
		} else {
			reporter.incrCounter(OutputData.D2Entities, 1);
		}
		output.collect(_key, toEmitFinal); 
		
		reporter.incrCounter(OutputData.BLOCK_ASSIGNMENTS, toEmit.size());
		//BC = BLOCK_ASSIGNMENTS / REDUCE_OUTPUT_RECORDS;		
	}
	

}
