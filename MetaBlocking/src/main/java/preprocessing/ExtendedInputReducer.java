/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ExtendedInputReducer extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, Text> {
	
	Text toEmit = new Text();
	
	static enum OutputData {CLEAN_BLOCKS};
	
	/**
	 * @param _key a block id (each element of the input value array) - 
	 * @param values a list of (entity id i, the ids and sizes of blocks containing this entity (Bi,|Bi|))
	 * @param output <b>key</b>: block id (same as input key). <b>value</b>: a textual concatenation of input int arrays 
	 *  
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {	
		
		StringBuilder toEmitBuffer = new StringBuilder();
		boolean atLeastTwoEntities = false;
		boolean containsNegative = false;
		boolean containsPositive = false;
		while (values.hasNext()) {			
			if (toEmitBuffer.length() > 1) {
				atLeastTwoEntities = true;
			}
			VIntWritable[] entityWithBlocks = values.next().get();
			toEmitBuffer.append(Arrays.toString(entityWithBlocks));
			if (!containsNegative && entityWithBlocks[0].get() < 0) {
				containsNegative = true;
			}
			if (!containsPositive && entityWithBlocks[0].get() >= 0) {
				containsPositive = true;
			}
		}
		if (atLeastTwoEntities) {
			toEmit.set(toEmitBuffer.toString()); 
			output.collect(_key, toEmit);
			if (containsNegative && containsPositive) { //a valid block for clean-clean ER
				reporter.incrCounter(OutputData.CLEAN_BLOCKS, 1);
			} //DIRTY_BLOCKS = REDUCE_OUTPUT_RECORDS
		} //PURGED_BLOCKS = REDUCE_INPUT_GROUPS - REDUCE_OUTPUT_RECORDS
	}

}
