/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import advanced.WEPMapper.Weight;


public class WEPReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
	double averageWeight;
	public void configure(JobConf conf) {
		long totalPairs;
		long totalWeight;
		try {
			JobClient client = new JobClient(conf);
	        RunningJob parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id")));        
	        totalPairs = parentJob.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getCounter();
	        totalWeight = parentJob.getCounters().getCounter(Weight.WEIGHT_COUNTER) / 1000;	
	        averageWeight = totalWeight / (double) totalPairs;
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
	
	public void reduce(Text _key, Iterator<DoubleWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			
		
		DoubleWritable weight = values.next(); //only one value
		if (weight.get() > averageWeight) {
			output.collect(_key, weight);
		}
		
		
	}

}
