/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WNPReducer extends MapReduceBase implements Reducer<VIntWritable, DoubleWritable, VIntWritable, DoubleWritable> {
		
	DoubleWritable averageWeight = new DoubleWritable();
	
	public void reduce(VIntWritable _key, Iterator<DoubleWritable> values,
			OutputCollector<VIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		double totalWeight = 0;
		int valuesCounter = 0;
		while (values.hasNext()) {			
			totalWeight += values.next().get();
			valuesCounter++;
		}
		averageWeight.set(totalWeight / valuesCounter);
		output.collect(_key, averageWeight);
	}

}
