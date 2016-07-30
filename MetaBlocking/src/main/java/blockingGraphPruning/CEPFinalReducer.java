/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CEPFinalReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	
	double minValue;
	int extraElements;
	public void configure(JobConf conf) {
		minValue = Double.parseDouble(conf.get("min", "0"));
		extraElements = conf.getInt("extra", 0);
	}

	public void reduce(DoubleWritable key, Iterator<Text> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {			
			
			if (key.get() == minValue) { //edge in topk+extraElements => skip ExtraElements
				int counter = 0;
				while (values.hasNext()) { //skip extraElements
					values.next();
					if (++counter == extraElements) {
						break;
					}
				}
				
			} 
			
			//output the rest of the edges
			while (values.hasNext()) {
				output.collect(values.next(), key);
			}
			
			
	}

}
