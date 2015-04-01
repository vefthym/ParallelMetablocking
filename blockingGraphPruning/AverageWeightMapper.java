package blockingGraphPruning;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class AverageWeightMapper extends MapReduceBase implements Mapper<Text, DoubleWritable, Text, DoubleWritable> {
	
	private Text commonKey = new Text("1"); 
	/* TODO: check the following alternatives for the common key
	byte testByte = 1;
	private ByteWritable commonKey = new ByteWritable(testByte);
	private VIntWritable commonKey = new VIntWritable(1);
	*/
	
	/**	 
	 * identity mapper - just keep a counter to sum up weights
	 * @param key i,j entity ids
	 * @param value wij the weight of this edge
	 * @param output identical to intput (identity mapper)
	 */
	public void map(Text key, DoubleWritable value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {		
		output.collect(commonKey, value); //common key for every 
	}

}
