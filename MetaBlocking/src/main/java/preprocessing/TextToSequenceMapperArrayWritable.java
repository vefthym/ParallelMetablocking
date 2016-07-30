/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class TextToSequenceMapperArrayWritable extends MapReduceBase implements Mapper<LongWritable, Text, VIntWritable, VIntArrayWritable> {

		
	//static enum InputData {NULL_ENTITY};
	
	VIntWritable blockId = new VIntWritable();
	
	/**
	 * input blocks: blockid (int) \t entity ids (ints, string separated)
	 * output the same with blockid as VIntWritable and block contents as Text
	 * 
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
	
		String[] block = value.toString().split("\t");
		blockId.set(Integer.parseInt(block[0]));
		String[] entities = (block[1].split("#"));
		List<VIntWritable> toEmitList = new ArrayList<>();
		for (String entity : entities) {			
			toEmitList.add(new VIntWritable(Integer.parseInt(entity)));
		}
		
		VIntWritable[] toEmitArray = new VIntWritable[toEmitList.size()];
		VIntArrayWritable toEmit = new VIntArrayWritable(toEmitList.toArray(toEmitArray));
		
		output.collect(blockId, toEmit);
	}
	
}
