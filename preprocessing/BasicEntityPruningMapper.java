package preprocessing;


import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BasicEntityPruningMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
  
	VIntArrayWritable toEmitFinal = new VIntArrayWritable();
	static enum InputData {NON_SINGLETON_INPUT, NON_SINGLETON_FOUND};

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
		Set<VIntWritable> entities = new TreeSet<>(); //dirty ER, sorts entity ids in ascending order

		StringTokenizer tok = new StringTokenizer(value.toString(),"#");		

		for (Integer entity = Integer.parseInt(tok.nextToken()); tok.hasMoreElements(); entity=Integer.parseInt(tok.nextToken())) {
			if (entity == null) { continue; }
			
			entities.add(new VIntWritable(entity));			
			reporter.progress();			
		}
		
		if (entities.size() < 2) {
			return;
		}
		
		VIntWritable[] entitiesArray = new VIntWritable[entities.size()];
		entitiesArray = entities.toArray(entitiesArray);		
		entities.clear(); //not needed anymore, free some memory		
		
		int currEntityIndex = 0;
		for (VIntWritable entity : entitiesArray) {
			if (currEntityIndex + 1 == entitiesArray.length) { return; }			
			reporter.setStatus((currEntityIndex+1) +"/"+entitiesArray.length+" block "+key);
			toEmitFinal.set(Arrays.copyOfRange(entitiesArray, currEntityIndex+1, entitiesArray.length));
			output.collect(entity, toEmitFinal); //toEmitFinal only contains greater entity ids than the key entity's id
			currEntityIndex++;
		}
		
	}

	
}
