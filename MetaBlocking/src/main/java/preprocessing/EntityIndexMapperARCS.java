/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityIndexMapperARCS extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {

	VIntWritable entityId = new VIntWritable();
	VIntWritable blockSize = new VIntWritable();
	VIntWritable[] compositeValueComponents = new VIntWritable[2];
	VIntArrayWritable compositeValue = new VIntArrayWritable();
	/**
	 * input: a blocking collection 
	 * @param inputKey block id
	 * @param value entity ids in this block, separated by "#"
	 * @param output 
	 * key: entity id (each of the input values)
	 * value: [blockId (i.e. inputKey), blockSize] (for ARCS).
	 */	
	public void map(VIntWritable inputKey, Text value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {

	//	String valueString = value.toString().replaceFirst(";", "");
		String []entities = value.toString().split("#");	
//		VIntWritable[] entities = value.get();
		blockSize.set(entities.length); 
		compositeValueComponents[0] = inputKey;
		compositeValueComponents[1] = blockSize;
		
		for (String entity : entities) {
			entityId.set(Integer.parseInt(entity));
			if (entityId == null) { continue; }			
			compositeValue.set(compositeValueComponents);
			output.collect(entityId, compositeValue);
		}		
	}

	
}
