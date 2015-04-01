package hadoopUtils;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InverseReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	
	public void reduce(DoubleWritable key, Iterator<Text> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {			
			
		while (values.hasNext()) {
			output.collect(values.next(), key);
		}	
	}

}
