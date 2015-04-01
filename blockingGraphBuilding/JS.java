/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphBuilding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JS extends MapReduceBase implements Reducer<Text, VIntWritable, Text, DoubleWritable> {

	
	public void reduce(Text _key, Iterator<VIntWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {		
		double sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		String[] inputKey = _key.toString().split(",");		
		StringBuffer outputKey = new StringBuffer(inputKey[0]);
		outputKey.append(",");
		outputKey.append(inputKey[2]);
		
		Integer Bi = Integer.parseInt(inputKey[1]);
		Integer Bj = Integer.parseInt(inputKey[3]);
		
		output.collect(new Text(outputKey.toString()), new DoubleWritable(sum/(Bi+Bj-sum)));
	}	
	
}
