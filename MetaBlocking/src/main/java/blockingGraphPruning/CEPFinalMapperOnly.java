/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class CEPFinalMapperOnly extends MapReduceBase implements Mapper<Text, DoubleWritable, Text, DoubleWritable> {
 
	double minValue;	
	
	public void configure(JobConf conf) {
		minValue = Double.parseDouble(conf.get("min", "0"));		
	}
	
	/**
	 * emit only edges that have value >= minValue (i.e. belong in top k edges)
	 */
	public void map(Text key, DoubleWritable value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		double weight = value.get();
		
		if (weight >= minValue) { //edge belongs in top k
			output.collect(key, value);
		}
	}

}
