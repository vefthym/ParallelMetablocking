package blockingGraphBuilding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class AllBlockComparisonsReducerDirty extends MapReduceBase implements Reducer<VIntWritable, Text, Text, VIntWritable> {
	
	private final static VIntWritable one = new VIntWritable(1);
	Text comparison = new Text();
	
	static enum OutputData {PURGED_BLOCKS};
	
	/**
	 * groups the "i,|Bi|" pairs (input values) of the block "_key" (input key) </br>
	 * outputs for each comparison i,j
	 * output key: i,|Bi|,j,|Bj|
	 * output value: 1
	 * output value (for ARCS): num of comparisons in the current block
	 * @param _key block id
	 * @param values a list of "i,|Bi|" pairs, where i is an entity id and Bi is the set of blocks that i is placed in </br>
	 * @param output key: i,|Bi|,j,|Bj| value: 1 (for ARCS: value = ||bk||)
	 */
	public void reduce(VIntWritable _key, Iterator<Text> values,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {	
		//List<String> D1entities = new ArrayList<>();
		//List<String> D2entities = new ArrayList<>();
		List<String> entities = new ArrayList<>(); //dirty ER
		
		reporter.setStatus("reducing "+_key);
		
		while (values.hasNext()) {
			entities.add(values.next().toString()); //dirty ER			
		}
//		int blockSize = entities.size(); //dirty ER (for ARCS)
		long numComparisons = (entities.size() * (entities.size()-1)) / 2; //dirty ER (for ARCS)	
		
//		long numComparisons = D1entities.size() * D2entities.size(); //clean-clean ER (for ARCS)
		
		if (numComparisons == 0) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;			
		}
		
		//clean-clean ER (comparisons)		
		/*for (String e1 : D1entities) {			
			for (String e2 : D2entities) {
				comparison.set(e1+","+e2);
				output.collect(comparison, one);				
			}
		}*/
		
		
		//dirty ER (comparisons)		
		for (String e1 : entities) {
			int e1val = Integer.parseInt(e1.substring(0,e1.indexOf(",")));
			for (String e2 : entities) {
				if (Integer.parseInt(e2.substring(0,e2.indexOf(","))) <= e1val) {continue;}
				comparison.set(e1+","+e2);
				output.collect(comparison, one);		
			}
		}
		
	}

}
