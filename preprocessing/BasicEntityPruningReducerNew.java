package preprocessing;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BasicEntityPruningReducerNew extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, NullWritable> {

	
	private final NullWritable NULL = NullWritable.get();
		
	VIntWritable fakeId = new VIntWritable(-1);	
	
	Set<VIntWritable> nonSingulars;
	public void configure (JobConf job) {
		nonSingulars = new HashSet<>();
	}
	
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, NullWritable> output, Reporter reporter) throws IOException {
		
		Set<VIntWritable> entities = new TreeSet<>();
		
		boolean first = true;
		boolean nonSingular = nonSingulars.contains(_key); //initially set to false
		
		while (values.hasNext()) {
			VIntArrayWritable nextValue = values.next();
			if (first && !values.hasNext() && !nonSingular) { //only one value => no repeated comparisons => singular entity	
				return;
			}
			first = false;
			VIntWritable[] next = nextValue.get();
			
			for (VIntWritable entity : next) {	
				reporter.progress();
				if (entity.equals(fakeId)) { 
					nonSingular = true;
					continue;
				}
				if (entities.add(entity) == false) { //entity is nonSingular
					nonSingular = true;
					reporter.progress();
					if (nonSingulars.add(entity) == true) { //added for the first time (at most once for each reducer)
						output.collect(entity, NULL); //at most once for each reducer
					}
				}				
			}
		}
		
		if (nonSingular) {			
			if (nonSingulars.add(_key) == true) { //added for the first time (at most once for each reducer)
				output.collect(_key, NULL); //at most once for each reducer
			}			
		}
		
		
	}	
}
