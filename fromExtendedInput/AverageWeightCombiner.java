package fromExtendedInput;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class AverageWeightCombiner extends MapReduceBase implements Reducer<ByteWritable, DoubleWritable, ByteWritable, DoubleWritable> {
	
	/**	 
	 * identity mapper - just keep a counter to sum up weights
	 * @param key i,j entity ids
	 * @param value wij the weight of this edge
	 * @param output identical to intput (identity mapper)
	 */
	public void reduce(ByteWritable key, Iterator<DoubleWritable> values,
			OutputCollector<ByteWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		double totalWeight = 0;
		while (values.hasNext()) {
			totalWeight += values.next().get();
		}		
		output.collect(key, new DoubleWritable(totalWeight));			
	}

}
