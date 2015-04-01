package blockingGraphBuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SumCombiner extends MapReduceBase implements Reducer<Text, VIntWritable, Text, VIntWritable> {

	//works for CBS,ECBS,JS
	public void reduce(Text _key, Iterator<VIntWritable> values,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {		
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}		
		output.collect(_key, new VIntWritable(sum));
	}	
	
}
