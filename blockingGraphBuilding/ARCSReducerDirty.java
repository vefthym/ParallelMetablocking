/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphBuilding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ARCSReducerDirty extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, Text, VLongWritable> {
	
	VLongWritable bk = new VLongWritable();
	Text comparison = new Text();
	
	static enum OutputData {PURGED_BLOCKS};
	
	/** 
	 * @param _key block id
	 * @param values a list of entity ids that belong to this block
	 * @param output key: i,j (entity ids) value: ||bk|| (block utility)
	 */
	@SuppressWarnings("unchecked")
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<Text, VLongWritable> output, Reporter reporter) throws IOException {	
		
		List<VIntWritable> entities = new ArrayList<>(); //dirty ER
		
		reporter.setStatus("reducing "+_key);
		
		while (values.hasNext()) {
			entities.add(values.next()); //dirty ER			
		}
		
		long numEntities = entities.size();

		long numComparisons = (numEntities * (numEntities-1)) / 2; //dirty ER (for ARCS)	
		
		if (numComparisons == 0) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;			
		}
		
		bk.set(numComparisons);
		
		Collections.sort(entities);
		
		//dirty ER (comparisons)	
		for (int i = 0; i < numEntities-1; ++i) {	
			reporter.setStatus(i+"/"+numEntities);
			int e1 = entities.get(i).get();			
			for (int j=i+1; j < numEntities; ++j) {
				int e2 = entities.get(j).get();				
				comparison.set(e1+","+e2);
				output.collect(comparison, bk);
			}
		}
	}

}
