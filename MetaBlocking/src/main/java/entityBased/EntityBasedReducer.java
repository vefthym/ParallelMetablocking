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

public class EntityBasedReducer extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {
	

	/**
	 * @param _key an entity id
	 * @param values the list of arrays with entity ids appearing in a block with the _key entity
	 * @param output the input with the values deduplicated (i.e., each entity appearing only once)
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new HashSet<>();		
		
		while (values.hasNext()) {
//			VIntWritable[] next = values.next().get(); //if not compressed
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next()); //if compressed
			entities.addAll(Arrays.asList(next));			
		}
		
		VIntWritable[] tmpArray = new VIntWritable[entities.size()];
		VIntArrayWritable toEmit = new VIntArrayWritable(entities.toArray(tmpArray));
		
		output.collect(_key, toEmit);
	}

}
