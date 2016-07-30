/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityPruningMapperNew extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
  
	//static enum InputData {BUF_LIMIT_REACHED};
	static enum InputData {NON_SINGLETON_INPUT, NON_SINGLETON_FOUND};
	
	VIntArrayWritable toEmitFinal = new VIntArrayWritable();
	
	//final int BUF_LIMIT = 10000; //write up to this number of values each time	
	/**
	 * input: a blocking collection
	 * input key: block id
	 * input value: entity ids in this block, separated by ","
	 * output key: entity id (each of the input values)
	 * output value: entity ids separated by " " (neighbors of output key)
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {

		reporter.setStatus("splitting the block "+key);
		List<VIntWritable> D1entities = new ArrayList<>();
		List<VIntWritable> D2entities = new ArrayList<>();
				
		String valueString = value.toString().replaceFirst(";", "");
		StringTokenizer tok = new StringTokenizer(valueString,"#");
		//List<VIntArrayWritable> buffer = new ArrayList<>();
				
//		int tmpCounter = 0;
		//split the bilateral block in two (clean-clean ER)
		for (Integer entity = Integer.parseInt(tok.nextToken()); tok.hasMoreElements(); entity=Integer.parseInt(tok.nextToken())) {
			if (entity == null) { continue; }
			if (entity > 0) {
				D1entities.add(new VIntWritable(entity));		
			} else {
				D2entities.add(new VIntWritable(entity));
				//outputValue.add(new VIntWritable(entity));
//				if (++tmpCounter == BUF_LIMIT) {
//					reporter.incrCounter(InputData.BUF_LIMIT_REACHED, 1);
//					
//					VIntWritable[] toEmitArray = new VIntWritable[outputValue.size()];
//					toEmitArray = outputValue.toArray(toEmitArray);					
//					buffer.add(new VIntArrayWritable(toEmitArray));
//					
//					outputValue = new ArrayList<>();
//					tmpCounter = 0;
//					
//					reporter.progress();
//				}
			}
			reporter.progress();			
		}
		
//		if (outputValue.size() > 1) {
//			VIntWritable[] toEmitArray = new VIntWritable[outputValue.size()];
//			toEmitArray = outputValue.toArray(toEmitArray);					
//			buffer.add(new VIntArrayWritable(toEmitArray));
//		}
		
		if (D1entities.isEmpty() || D2entities.isEmpty()) {
			return;
		}
		
		VIntWritable[] toEmitD1Array = new VIntWritable[D1entities.size()];
		toEmitD1Array = D1entities.toArray(toEmitD1Array);
		reporter.progress();
		toEmitFinal.set(toEmitD1Array);
		
		reporter.setStatus("finding the D2 comparisons of "+key);		
		for (VIntWritable e2 : D2entities) {			
//			for (VIntArrayWritable bufferedArray : buffer) { //emit all stored neighbors
			output.collect(e2, toEmitFinal);
//			}
		}
		
		//do the same for D1
		VIntWritable[] toEmitD2Array = new VIntWritable[D2entities.size()];
		toEmitD1Array = D2entities.toArray(toEmitD2Array);
		reporter.progress();
		toEmitFinal.set(toEmitD2Array);
		
		reporter.setStatus("finding the D1 comparisons of "+key);		
		for (VIntWritable e1 : D1entities) {			
//			for (VIntArrayWritable bufferedArray : buffer) { //emit all stored neighbors
			output.collect(e1, toEmitFinal);
//			}
		}
		 
		
		
		
		
		
	}

	
}
