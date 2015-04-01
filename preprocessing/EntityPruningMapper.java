package preprocessing;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityPruningMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
  
	static enum InputData {BUF_LIMIT_REACHED};
	
	//VIntArrayWritable toEmitFinal = new VIntArrayWritable();
	
	final int BUF_LIMIT = 10000; //write up to this number of values each time	
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
		//String []entities = value.toString().split(",");
		List<VIntWritable> D1entities = new ArrayList<>();
		//List<VIntWritable> D2entities = new ArrayList<>();
				
		String valueString = value.toString().replaceFirst(";", "");
		StringTokenizer tok = new StringTokenizer(valueString,"#");
		List<VIntArrayWritable> buffer = new ArrayList<>();
		
		//StringBuffer outputValue = new StringBuffer();
		List<VIntWritable> outputValue = new ArrayList<>();
				
		int tmpCounter = 0;
		//split the bilateral block in two (clean-clean ER)
		for (Integer entity = Integer.parseInt(tok.nextToken()); tok.hasMoreElements(); entity=Integer.parseInt(tok.nextToken())) {
			if (entity == null) { continue; }
			if (entity > 0) {
				D1entities.add(new VIntWritable(entity));				
			} else {
				//D2entities.add(new VIntWritable(entity));
				outputValue.add(new VIntWritable(entity));
				if (++tmpCounter == BUF_LIMIT) {
					reporter.incrCounter(InputData.BUF_LIMIT_REACHED, 1);
					
					VIntWritable[] toEmitArray = new VIntWritable[outputValue.size()];
					toEmitArray = outputValue.toArray(toEmitArray);					
					buffer.add(new VIntArrayWritable(toEmitArray));
					
					outputValue = new ArrayList<>();
					tmpCounter = 0;
					
					reporter.progress();
				}
			}
			reporter.progress();			
		}
		
		if (outputValue.size() > 1) {
			VIntWritable[] toEmitArray = new VIntWritable[outputValue.size()];
			toEmitArray = outputValue.toArray(toEmitArray);					
			buffer.add(new VIntArrayWritable(toEmitArray));
		}
		
		if (buffer.isEmpty() || D1entities.isEmpty()) {
			return;
		}
		
//		VIntWritable[] toEmitArray = new VIntWritable[outputValue.size()];
//		toEmitArray = outputValue.toArray(toEmitArray);
//		reporter.progress();
//		toEmitFinal.set(toEmitArray);		
				
		int counter = 0;
		for (VIntWritable e1 : D1entities) {
			reporter.setStatus(++counter+"/"+D1entities.size());
			for (VIntArrayWritable bufferedArray : buffer) { //emit all stored neighbors
				output.collect(e1, bufferedArray);
			}
		}
		 
		
		
		
		
		
	}

	
}
