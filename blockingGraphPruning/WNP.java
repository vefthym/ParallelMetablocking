package blockingGraphPruning;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


//common for WNP and CNP
public class WNP extends MapReduceBase implements Reducer<VIntWritable, Text, Text, DoubleWritable> {
		
	public enum Output {NUM_RECORDS};
	
	Text comparison = new Text();
	DoubleWritable weight = new DoubleWritable();
	
	/**	 
	 * output for each input node its edges with weight above a local threshold (avg threshold)
	 * @param key i entity id
	 * @param value list of j,wij (entity id, weight of edge i-j)
	 * @param output key:i,j value:wij for wij > thresh 
	 */
	public void reduce(VIntWritable key, Iterator<Text> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {		
			double localWeights = 0;
			double localThresh;
			
			Map<Integer,Double> neighbors = new HashMap<>();
			
			//find local thresh
			while (values.hasNext()) {
				String[] value = values.next().toString().split(",");
				Double weight = Double.parseDouble(value[1]);
				neighbors.put(Integer.parseInt(value[0]), weight);
				localWeights += weight;
			}
			localThresh = localWeights / neighbors.size(); //the average weight
			
			//start emitting pruned edges
			for (Map.Entry<Integer, Double> neighbor : neighbors.entrySet()) {
				if (neighbor.getValue() > localThresh) {
					if (key.get() >= 0)  { //to make sure they will go to the same reducer (reciprocal)						
						comparison.set(key+","+neighbor.getKey());
					} else {						
						comparison.set(neighbor.getKey()+","+key);
					}
					weight.set(neighbor.getValue());
					//output.collect(comparison, weight);
					reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space
				}
			}
			
	}

}
