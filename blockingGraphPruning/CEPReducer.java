package blockingGraphPruning;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CEPReducer extends MapReduceBase implements Reducer<DoubleWritable, VIntWritable, DoubleWritable, NullWritable> {
	
	private int k;	
	private int counter;
	
	public void configure (JobConf conf) {
		k = conf.getInt("K", 1000000);
		counter = 0;
	}
	
	//get keys (weights) in descending order
	public void reduce(DoubleWritable key, Iterator<VIntWritable> values,
			OutputCollector<DoubleWritable, NullWritable> output, Reporter reporter) throws IOException {
			if (counter < k) {
				int numComparisonsWithThisWeight = 0;
				while (values.hasNext()) {
					numComparisonsWithThisWeight += values.next().get();
				}
				
				counter += numComparisonsWithThisWeight;
				
				if (counter >= k) { //entered only once 
					output.collect(key, NullWritable.get()); //the minimum value (edges with greater value are in top k)
					output.collect(new DoubleWritable(counter-k), NullWritable.get()); //#additional elements (how many more than k)
				}
			} //else we don't care, we have found the top-K scores
			
	}

}
