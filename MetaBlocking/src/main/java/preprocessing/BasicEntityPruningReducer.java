/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BasicEntityPruningReducer extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, NullWritable> {

	
private final NullWritable NULL = NullWritable.get();
	
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, NullWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new HashSet<>();
		boolean first = true;
		
		while (values.hasNext()) {
			VIntArrayWritable nextValue = values.next();
			if (first && !values.hasNext()) { //only one value => no repeated comparisons => singular				
				return;
			}
			first = false;
			VIntWritable[] next = nextValue.get();
			
			for (VIntWritable entity : next) {				
				if (entities.add(entity) == false) { //entity is nonSingular
					output.collect(_key, NULL); //emit the entity as nonSingular (only once)
					return;
				}
				
			}
		}
	}
	
	/*public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
	OutputCollector<VIntWritable, NullWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new HashSet<>();		
		
		while (values.hasNext()) {
			
			VIntWritable next = values.next();
			
			if (next.get() == -1) {
				output.collect(_key, NULL);
				return;
			}
			
			if (entities.add(next) == false) { //entity is nonSingular
				output.collect(_key, NULL); //emit the entity as nonSingular (only once)
				return;
			}
			
		}
	}*/
}
