/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EJSReducer extends MapReduceBase implements Reducer<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
		
	VIntArrayWritable toEmit = new VIntArrayWritable();
	
	/**
	 * @param _key entityId
	 * @param values a list of concatenations of blockId,#distinctComparisonsOfentityId in this block
	 * @param output key: blockId for each blockId in the values. value: [entityId,blockIds,...,#totalComparisonsOfEid]  
	 */
	public void reduce(VIntWritable _key, Iterator<Text> values,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
	
		List<VIntWritable> blocks = new ArrayList<>(); 
		int comparisons = 0;
		while (values.hasNext()) {			
			String[] value = values.next().toString().split(",");
			blocks.add(new VIntWritable(Integer.parseInt(value[0])));
			comparisons += Integer.parseInt(value[1]);
		}
		
		VIntWritable[] toEmitArray = new VIntWritable[blocks.size()+2];
		toEmitArray[0] = _key; 													//first index->Eid
		System.arraycopy(blocks.toArray(), 0, toEmitArray, 1, blocks.size());	//middle ->block
		toEmitArray[blocks.size()+1] = new VIntWritable(comparisons);			//last index->#comparisons
		
		toEmit.set(toEmitArray);
		
		for (VIntWritable blockId : blocks) {
			output.collect(blockId, toEmit);
		}
	
	
	}

}
