package advanced;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CNP extends MapReduceBase implements Reducer<VIntWritable, Text, Text, DoubleWritable> {
	
	
	private int k; //for topK
	public void configure (JobConf job) {
		float BCin = job.getFloat("BCin", 1.0f);
		k = ((Double)Math.floor(BCin - 1)).intValue();
	}
	
	Text keyToEmit = new Text();
	DoubleWritable valueToEmit = new DoubleWritable();
		
	/**	 
	 * output for each input node its edges with weight in top k weights
	 * @param key i entity id
	 * @param value list of j,wij (entity id, weight of edge i-j)
	 * @param output key:i,j value:wij for wij in top k weights
	 */
	public void reduce(VIntWritable key, Iterator<Text> values,
		OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {				
		
		//sort neighbors in descending order of weight (key=weigh, value=neighborID)
		Map<Double,Integer> neighbors = new TreeMap<>(Collections.reverseOrder());
		
		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			Double weight = Double.parseDouble(value[1]);
			Integer neighbor = key.get() + Integer.parseInt(value[0]); //e2 is compressed as e2-e1, so we add e1 (i.e. key)
			neighbors.put(weight, neighbor);
		}			

		int keyId = key.get();
		
		//Emit top k edges (k nearest neighbors)
		for (Map.Entry<Double, Integer> edge : neighbors.entrySet()) {
			if (k-- == 0) { return; }
			if (keyId > edge.getValue())  { //to make sure they will go to the same (next) reducer (of reciprocal)
				keyToEmit.set(keyId+","+edge.getValue());				
			} else {
				keyToEmit.set(edge.getValue()+","+keyId);				
			}		
			valueToEmit.set(edge.getKey());
			output.collect(keyToEmit, valueToEmit);
		}
			
	}

}
