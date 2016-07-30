/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;

import hadoopUtils.RelativePositionCompression;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedMapperFromCompressedNP extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {

	VIntArrayWritable toEmit = new VIntArrayWritable();
	/**
	 * input: a blocking collection
	 * @param key block id
	 * @param value entity ids in this block
	 * @param output key: entity id (each of the input values)
	 * 				 value: an array with all other entities (ids) in this block
	 */	
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		VIntWritable[] entities = value.get();	
		Arrays.sort(entities);		
		VIntArrayWritable outputEntities = RelativePositionCompression.compress(entities);
		for (int i = 0; i < entities.length; ++i) { 
			reporter.setStatus((i+1)+"/"+entities.length);
			output.collect(entities[i], outputEntities);
		}	
	}
}
