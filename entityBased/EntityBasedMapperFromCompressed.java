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

public class EntityBasedMapperFromCompressed extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {

	VIntArrayWritable toEmit = new VIntArrayWritable();
	/**
	 * input: a blocking collection
	 * input key: block id
	 * input value: entity ids in this block
	 * output key: entity id (each of the input values)
	 * output value: an array with all other entities (ids) in this block
	 */	
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		VIntWritable[] entities = value.get();	
		
		Arrays.sort(entities); //do it once, to save doing it in compression
		

//		for (int i = 0; i < entities.length; ++i) { //when all entities are copied (OPTION 1)
		for (int i = 0; i < entities.length-1; ++i) { //when bigger entities are copied (OPTION 2)
			reporter.setStatus((i+1)+"/"+entities.length);
			//OPTION 1: copy all elements before and after i
//			VIntWritable[] tmpEntities = new VIntWritable[entities.length-1]; //all other entities, except the one at index i
//			
//			System.arraycopy(entities, 0, tmpEntities, 0, i); //all elements before i
//			System.arraycopy(entities, i+1, tmpEntities, i, entities.length-i-1); //all elements after i
			
			//OPTION 2: copy only the elements after i
			VIntWritable[] tmpEntities = new VIntWritable[entities.length-i-1]; //all BIGGER entities
			System.arraycopy(entities, i+1, tmpEntities, 0, tmpEntities.length); //all elements after i
						
			output.collect(entities[i], RelativePositionCompression.compress(tmpEntities));
		}
		
			
	}

	
}
