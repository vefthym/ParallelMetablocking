/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {

	VIntArrayWritable toEmit = new VIntArrayWritable();
	/**
	 * input: a blocking collection
	 * input key: block id
	 * input value: entity ids in this block, separated by "#"
	 * output key: entity id (each of the input values)
	 * output value: an array with all other entities (ids) in this block
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {

//		String valueString = value.toString().replaceFirst(";", ""); //clean
//		String []entities = valueString.split("#");	//clean
		String []entities = value.toString().split("#"); //dirty	
//		VIntWritable[] entities = value.get();
		
//		VIntWritable[] array = new VIntWritable[entities.length];
		List<VIntWritable> entityList = new ArrayList<>(entities.length);
		
		for (int i = 0; i < entities.length; ++i) {			
			entityList.add(i, new VIntWritable(Integer.parseInt(entities[i])));	
		}
		
		for (int i = 0; i < entities.length; ++i) {
			reporter.setStatus((i+1)+"/"+entities.length);
			List<VIntWritable> tmp = new ArrayList<>(entityList);
			tmp.remove(i); //remove element at position i (not entity with id i)
			
			
			VIntWritable[] tmpArray = new VIntWritable[tmp.size()];
			toEmit.set(tmp.toArray(tmpArray));
//			VIntArrayWritable toEmit = new VIntArrayWritable((VIntWritable[])tmp.toArray());			
			output.collect(entityList.get(i), toEmit);
		}
		
			
	}

	
}
