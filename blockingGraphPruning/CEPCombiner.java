/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CEPCombiner extends MapReduceBase implements Reducer<DoubleWritable, VIntWritable, DoubleWritable, VIntWritable> {

	VIntWritable toEmit = new VIntWritable();
	
	//get keys (weights) in descending order
	public void reduce(DoubleWritable key, Iterator<VIntWritable> values,
			OutputCollector<DoubleWritable, VIntWritable> output, Reporter reporter) throws IOException {
			
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			toEmit.set(sum);
			output.collect(key, toEmit);
	}

}
