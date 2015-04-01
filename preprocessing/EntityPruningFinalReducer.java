package preprocessing;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EntityPruningFinalReducer extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, Text> {
		
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {
		
		reporter.setStatus("reducing "+_key);		
		StringBuffer toEmit = new StringBuffer();
		boolean unary = true;		
		while (values.hasNext()) {			
			toEmit.append("#"+values.next().get());
			reporter.progress();			
			if (values.hasNext()) { unary = false; }
		}
		if (!unary) {
			output.collect(_key, new Text(toEmit.toString().substring(1))); //substring(1) to remove the first ','
		}
	}

}
