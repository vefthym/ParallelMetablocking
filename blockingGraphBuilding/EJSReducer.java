package blockingGraphBuilding;

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

public class EJSReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	Text comparison = new Text();
	Text JSvi = new Text();
	/**
	 * @param key i,|Bi| where i is an enity id and |Bi| its entity index size
	 * @param value list of j,|Bj| (non-distinct)
	 * @param output key: i,j value: JS,|vi| 
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		Map<String,Integer> neighbors = new HashMap<>(); //map value is |Bij|,i.e., #common blocks
		
		String[] keyString = _key.toString().split(",");
		Long i = Long.parseLong(keyString[0]);
		Integer Bi = Integer.parseInt(keyString[1]);
		
		while (values.hasNext()) {
			String mapKey = values.next().toString();
			Integer mapValue = neighbors.get(mapKey);
			if (mapValue == null) { //first time this neighbor appears
				mapValue = 0;
			}			
			neighbors.put(mapKey, ++mapValue);
		}
		
		int vi = neighbors.size();
		for (Map.Entry<String, Integer> neighbor : neighbors.entrySet()) {
			String[] valueString = neighbor.getKey().split(",");
			Long j = Long.parseLong(valueString[0]);
			Integer Bj = Integer.parseInt(valueString[1]);
			Integer Bij = neighbor.getValue();
			double JS = (double) Bij / (Bi+Bj-Bij);
			JSvi.set(JS+","+vi);
			if (i > j) { //to ensure that both vi and vj will go to the same reduce task in the next job				
				comparison.set(i+","+j);
			} else {
				comparison.set(j+","+i);				
			}
			output.collect(comparison, JSvi);
		}	
	}	
	
}
