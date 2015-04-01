package preprocessing;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BasicEntityPruningCombiner extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {

	VIntWritable fakeId = new VIntWritable(-1);
	VIntArrayWritable toEmit = new VIntArrayWritable();
	
	Set<VIntWritable> nonSingulars;
	public void configure (JobConf job) {
		nonSingulars = new HashSet<>();
	}
	
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new TreeSet<>();
		
		boolean first = true;
		boolean nonSingular = nonSingulars.contains(_key); //initially set to false
		
		while (values.hasNext()) {
			VIntArrayWritable nextValue = values.next();
			if (first && !values.hasNext() && !nonSingular) { //only one value => no repeated comparisons yet				
				output.collect(_key, nextValue); //just output the input
				return;
			}
			first = false;
			VIntWritable[] next = nextValue.get();
			
			for (VIntWritable entity : next) {	
				reporter.progress();
				if (entity.equals(fakeId)) { continue; }
				if (entities.add(entity) == false) { //entity is nonSingular
					nonSingular = true;
					nonSingulars.add(entity);					
				}				
			}
		}
		
		if (nonSingular) {
			entities.removeAll(nonSingulars); //entities are now only singulars (i.e. without known nonSingulars)
			reporter.progress();
			entities.add(fakeId); //to denote that this entity is nonSingular			
			nonSingulars.add(_key);
		}
		
		VIntWritable[] entitiesArray = new VIntWritable[entities.size()];
		entitiesArray = entities.toArray(entitiesArray);
		toEmit.set(entitiesArray);
		output.collect(_key, toEmit);
		
	}	
}
