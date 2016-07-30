/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.BasicEntityPruningMapper.InputData;

public class EntityPruningCombiner extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VIntWritable> {
	
	//Text next = new Text();
	//VIntArrayWritable toEmit = new VIntArrayWritable();
	//VIntArrayWritable empty = new VIntArrayWritable(new VIntWritable[0]);
	VIntWritable dummy = new VIntWritable(-1);
	
	
	/**
	 * merges two sets of entity neighbors into one list (containing duplicates with max 2 occurences)
	 */
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
	OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new HashSet<>();
	//	boolean first = true;
				
		while (values.hasNext()) {
			//VIntArrayWritable nextValue = values.next();
			VIntWritable next = values.next();
//			if (first && !values.hasNext()) { //only one value => forward input to reducer
//				output.collect(_key, next);
//				return;
//			}
			//VIntWritable[] next = nextValue.get();
			reporter.progress();
			//first = false;
			
//			if (next.length == 0) {
//				output.collect(_key, empty);
//				reporter.incrCounter(InputData.NON_SINGLETON_INPUT, 1);
//				return;
//			}
			if (next.get() == -1) {
				output.collect(_key, dummy);
				reporter.incrCounter(InputData.NON_SINGLETON_INPUT, 1);
				return;
			}
			
//			for (VIntWritable entity : next) {			
			reporter.progress();
			if (entities.add(next) == false) { //entity is nonSingular		
				output.collect(_key, dummy);
				reporter.incrCounter(InputData.NON_SINGLETON_FOUND, 1);
				return;
			}
//			}
		}		
	
		//at this point, all the neighbor entities are unique, so continue...
//		VIntWritable[] toEmitArray = new VIntWritable[entities.size()];
//		toEmitArray = entities.toArray(toEmitArray);		
//		reporter.progress();
//		toEmit.set(toEmitArray);
//		
//		output.collect(_key, toEmit);		
		for (VIntWritable entity: entities) {
			output.collect(_key, entity);
		}
	}
}
