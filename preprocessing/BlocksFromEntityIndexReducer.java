/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class BlocksFromEntityIndexReducer extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> {
	
	static enum OutputData {CLEAN_BLOCKS};
	
	/**
	 * @param _key a block id (each element of the input value array) - 
	 * @param values a list of entity ids i)
	 * @param output <b>key</b>: block id (same as input key). <b>value</b>: a concatenation of input int arrays 
	 *  
	 */
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {	
		
		List<VIntWritable> inputList = new ArrayList<>();
		boolean atLeastTwoEntities = false;
		boolean containsNegative = false;
		boolean containsPositive = false;
		while (values.hasNext()) {			
			if (inputList.size() > 1) {
				atLeastTwoEntities = true;
			}			
			int next = values.next().get();
			inputList.add(new VIntWritable(next));

			if (!containsNegative && next < 0) {
				containsNegative = true;
			}
			if (!containsPositive && next >= 0) {
				containsPositive = true;
			}
		}
		if (atLeastTwoEntities) { //else purge this block			
			VIntWritable[] tmpArray = new VIntWritable[inputList.size()];
			 
//			output.collect(_key, new VIntArrayWritable(inputList.toArray(tmpArray))); //dirty
			if (containsNegative && containsPositive) { //a valid block for clean-clean ER
				output.collect(_key, new VIntArrayWritable(inputList.toArray(tmpArray))); //clean-clean
				reporter.incrCounter(OutputData.CLEAN_BLOCKS, 1);
			} //DIRTY_BLOCKS = REDUCE_OUTPUT_RECORDS
		} //PURGED_BLOCKS = REDUCE_INPUT_GROUPS - REDUCE_OUTPUT_RECORDS
	}

}
