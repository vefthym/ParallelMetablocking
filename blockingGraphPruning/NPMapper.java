/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

//common for WNP and CNP
public class NPMapper extends MapReduceBase implements Mapper<Text, DoubleWritable, VIntWritable, Text> {
		
	VIntWritable i = new VIntWritable();
	VIntWritable j = new VIntWritable();
	Text iWij = new Text();
	Text jWij = new Text();
	
	/**	 
	 * output for each input edge both its nodes as keys and the other node and weight as value
	 * @param key i,j entity ids
	 * @param value wij the weight of this edge
	 * @param output key:i value:j,wij and the inverse (key:j value:i,wij) 
	 */
	public void map(Text key, DoubleWritable value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {		
		String[] comparison = key.toString().split(",");
		i.set(Integer.parseInt(comparison[0]));
		j.set(Integer.parseInt(comparison[1]));
		iWij.set(i+","+value);
		jWij.set(j+","+value);
		
		output.collect(i, jWij);
		output.collect(j, iWij);		
	}

}
