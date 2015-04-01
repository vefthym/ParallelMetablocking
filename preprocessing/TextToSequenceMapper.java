package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class TextToSequenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, VIntWritable, Text> {

		
	//static enum InputData {NULL_ENTITY};
	
	VIntWritable blockId = new VIntWritable();	
	Text entities = new Text();
	/**
	 * input blocks: blockid (int) \t entity ids (ints, string separated)
	 * output the same with blockid as VIntWritable and block contents as Text
	 * 
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {
		
	
		String[] block = value.toString().split("\t");
		blockId.set(Integer.parseInt(block[0]));
		entities.set(block[1]);
		//if (blockId.get() == 950828) { //FIXME: delete (just used for debugging)		
//			reporter.setStatus(entities.toString());
//			blockId.set(1); //just for debugging
//			entities.set("1"); //just for debugging			
		//}
		output.collect(blockId, entities);
	}
	
}
