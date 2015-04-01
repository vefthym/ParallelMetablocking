package blockingGraphBuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ARCS extends MapReduceBase implements Reducer<Text, VLongWritable, Text, DoubleWritable> {

	/**
	 * @param _key i,j entity ids
	 * @param values list of ||bk||, where bk in Bij (the cardinalities of all common blocks)
	 * @param output key: same as input key. value: wij (ARCS) = sum_k(1/||bk||)
	 */
	public void reduce(Text _key, Iterator<VLongWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {		
		double sum = 0;
		while (values.hasNext()) {
			sum += 1.0 / values.next().get();
		}		
		output.collect(_key, new DoubleWritable(sum));
	}	
	
}
