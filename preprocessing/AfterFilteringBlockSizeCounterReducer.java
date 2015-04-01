/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AfterFilteringBlockSizeCounterReducer extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VIntWritable> {

	
	private VIntWritable finalSum = new VIntWritable();
	
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {				
	
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}	
		
		finalSum.set(sum);
		output.collect(_key, finalSum);  //also outputs finalSum 1
		

	}
	

}
