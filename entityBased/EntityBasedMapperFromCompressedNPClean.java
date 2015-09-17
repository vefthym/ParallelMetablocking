/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;

import hadoopUtils.RelativePositionCompression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedMapperFromCompressedNPClean extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {

	VIntArrayWritable toEmit = new VIntArrayWritable();
	/**
	 * input: a blocking collection
	 * @param key block id
	 * @param value entity ids in this block
	 * @param output key: entity id (each of the input values)
	 * 				 value: an array with all other entities (ids) in this block
	 */	
	@SuppressWarnings("unchecked")
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		VIntWritable[] entities = value.get();	
		
		//separate positives from negatives
		List<VIntWritable> positives = new ArrayList<>();
		List<VIntWritable> negatives = new ArrayList<>();
		
		for (int i = 0; i < entities.length; ++i) { 
			if (entities[i].get() < 0) {
				negatives.add(entities[i]);
			} else {
				positives.add(entities[i]);
			}
		}
		
		if (positives.isEmpty() || negatives.isEmpty()) {
			return; //purged block (no comparisons)
		}
		
		Collections.sort(positives); //sort positives in ascending order
		Collections.sort(negatives, Collections.reverseOrder()); //sort negatives in descending order (saves more space in compression)

		//compress the two arrays once
		VIntWritable[] positivesArray = new VIntWritable[positives.size()];
		VIntWritable[] negativesArray = new VIntWritable[negatives.size()];		
		VIntArrayWritable positiveEntities = RelativePositionCompression.compress(positives.toArray(positivesArray));
		VIntArrayWritable negativeEntities = RelativePositionCompression.compress(negatives.toArray(negativesArray));
	
		//emit all the negative entities array (compressed) for each positive entity
		for (int i = 0; i < positivesArray.length; ++i) {
			reporter.setStatus((i+1)+"/"+positivesArray.length+" positives");
			output.collect(positivesArray[i], negativeEntities);
		}
		
		//emit all the positive entities array (compressed) for each negative entity
		for (int i = 0; i < negativesArray.length; ++i) {
			reporter.setStatus((i+1)+"/"+negativesArray.length+" negatives");
			output.collect(negativesArray[i], positiveEntities);
		}
	}
}
