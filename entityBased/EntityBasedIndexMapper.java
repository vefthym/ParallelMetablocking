/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityBasedIndexMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntWritable> {

	VIntWritable entityId = new VIntWritable();
	/**
	 * input: a blocking collection
	 * input key: block id
	 * input value: entity ids in this block, separated by "#"
	 * output key: entity id (each of the input values)
	 * output value: block id (the same as the input key)
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {

//		String valueString = value.toString().replaceFirst(";", ""); //clean
//		String []entities = valueString.split("#");	//clean
		String []entities = value.toString().split("#"); //dirty	
//		VIntWritable[] entities = value.get();
		
		for (String entity : entities) {
			entityId.set(Integer.parseInt(entity));
//			if (entity == null) { continue; }			
			output.collect(entityId, key);
		}		
	}

	
}
