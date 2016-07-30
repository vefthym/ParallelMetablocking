/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EntityPruningReducer extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, NullWritable> {

	
	private final NullWritable NULL = NullWritable.get();
	
	
	private Set <VIntWritable> nonSingulars; 
	public void configure (JobConf job) {
		nonSingulars = new HashSet<>();
	}
	
	
	/**
	 * Removes singular entities from the blocks
	 * input _key: enitty id
	 * input values: entities sharing a block with the key entity
	 * output key: entity id of the non-singular entities
	 * output value: nothing 
	 */
	/*
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, NullWritable> output, Reporter reporter) throws IOException {
		
	Set<VIntWritable> entities = new HashSet<>();
	Set<VIntWritable> blocks = new HashSet<>();
	
	boolean singular = true;
	
	while (values.hasNext()) {
		String[] value = values.next().toString().split(" ");
		VIntWritable entity = new VIntWritable(Integer.parseInt(value[0]));
		if (singular) {
			if (entities.add(entity) == false) { //comparison is repeated
				singular = false;
			}
		}
		blocks.add(new VIntWritable(Integer.parseInt(value[1])));
	}
	
	if (!singular) {
		for (VIntWritable block : blocks) {
			output.collect(block, _key);
		}
	}
}
	*/
		
	
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, NullWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new HashSet<>();		 
		
		boolean singular = true;
		while (values.hasNext()) {
			VIntWritable[] next = values.next().get();
			for (VIntWritable entity : next) {				
				if (entities.add(entity) == false) { //entity is nonSingular
					singular = false;					
					if (nonSingulars.add(entity) == true) { //nonSingular entity added for first time (in this reducer!)
						output.collect(entity, NULL); //emit the entity as nonSingular (only once)
					}
				}
				
			}
		}
		
		if (!singular) {
			if (nonSingulars.add(_key) == true) { //nonSingular entity added for first time (in this reducer!)
				output.collect(_key, NullWritable.get());
			}
		}
	}
		
		
		//SOLUTION 2
		//DO NOT DELETE! KEEP FOR BACKUP (WORKING!) SOLUTION
		/*values.next(); //no need to check here; there is always at least one value
		if (values.hasNext()) { //comparison appears at least twice
			String[] comparison = _key.toString().split(" ");
			String e1 = comparison[0];
			String e2 = comparison[1];
			output.collect(new Text(e1), NullWritable.get());
			output.collect(new Text(e2), NullWritable.get());
		}
		
		
	}*/

}
