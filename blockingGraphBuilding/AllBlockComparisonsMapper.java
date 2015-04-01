/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphBuilding;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;


public class AllBlockComparisonsMapper extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, Text> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, MALFORMED_PAIRS};
		
	Text outputValue = new Text();
	/**
	 * maps an input entity index into (key, value) pair(s)
	 * the value is the entity id (input key) along with the num of blocks that contain it
	 * the key each time is a block id (each element of the input value array)
	 * @param key an entity id
	 * @param value an array of block ids that this entity belongs to
	 * @param output key: a block id (each element of the input value array) - value: the entity id (input key), its entity index size (|Bi|) 
	 * along with the num of blocks that contain it
	 */
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {
		 
		VIntWritable [] Bi = value.get(); 	
		int BiSize = Bi.length;
		for (VIntWritable bi : Bi) {
			outputValue.set(key+","+BiSize);
			output.collect(bi, outputValue);			
		}
		
	}

}
