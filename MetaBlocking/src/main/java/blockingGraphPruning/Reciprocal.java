/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//common for WNP and CNP
public class Reciprocal extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	DoubleWritable weight = new DoubleWritable();
	
	/**	 
	 * output from the input comparisons (with either one or two values) those with two values
	 * @param key i,j entity ids (comparison)
	 * @param value list of one or two wij (weight of edge i-j)
	 * @param output key:i,j (same as input key) value:wij for the keys that have two values 
	 */
	public void reduce(Text key, Iterator<DoubleWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {			
						
			weight = values.next(); //the first value
			if (values.hasNext()) { //the edge is reciprocal (two identical values)
				output.collect(key, weight);
			}
			
			
	}

}
