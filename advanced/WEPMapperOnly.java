package advanced;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WEPMapperOnly extends MapReduceBase implements Mapper<Text, DoubleWritable, Text, DoubleWritable> {
  
	private double averageWeight;
	
	public enum OutputData {OUTPUT_RECORDS};
	
		
	public void configure (JobConf job) {		
		averageWeight = Double.parseDouble(job.get("averageWeight")); 		
	}
	
	public void map(Text key, DoubleWritable value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);		
				
		if (value.get() > averageWeight) {
			reporter.incrCounter(OutputData.OUTPUT_RECORDS, 1);
//			output.collect(key, value);
		}
	}
	
}
