/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

public class AverageWeightReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, DoubleWritable, NullWritable> {

	long totalPairs;
	public void configure(JobConf conf) {
		try {
			JobClient client = new JobClient(conf);
	        RunningJob parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id")));        
	        totalPairs = parentJob.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getCounter();			
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
	
	/**	 
	 * identity mapper - just keep a counter to sum up weights
	 * @param key i,j entity ids
	 * @param value wij the weight of this edge
	 * @param output identical to input (identity mapper)
	 */
	public void reduce(Text key, Iterator<DoubleWritable> values,
			OutputCollector<DoubleWritable, NullWritable> output, Reporter reporter) throws IOException {
		double totalWeight = 0;
		while (values.hasNext()) {
			totalWeight += values.next().get();
		}
		output.collect(new DoubleWritable(totalWeight/totalPairs), NullWritable.get());			
	}

}
