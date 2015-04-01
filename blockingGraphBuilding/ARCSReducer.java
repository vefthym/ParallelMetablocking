package blockingGraphBuilding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ARCSReducer extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, Text, VLongWritable> {
			
	static enum OutputData {PURGED_BLOCKS};
	
	VLongWritable bk = new VLongWritable();
	Text comparison = new Text();
	
	/** 
	 * @param _key block id
	 * @param values a list of entity ids that belong to this block
	 * @param output key: i,j (entity ids) value: ||bk|| (block utility)
	 */
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<Text, VLongWritable> output, Reporter reporter) throws IOException {	
		List<Integer> D1entities = new ArrayList<>();
		List<Integer> D2entities = new ArrayList<>();
		//List<Long> entities = new ArrayList<>(); //dirty ER
		
		reporter.setStatus("reducing "+_key);
		
		while (values.hasNext()) {
			//entities.add(values.next().get()); //dirty ER
			int entity = values.next().get();			
			if (entity >= 0) {
				D1entities.add(entity);				
			} else {
				D2entities.add(entity);
			}
			reporter.progress();
		}
//		int blockSize = entities.size(); //dirty ER 
//		long numComparisons = ((long)blockSize * (blockSize-1)) / 2; //dirty ER
		
		long numComparisons = (long) D1entities.size() * (long) D2entities.size(); //clean-clean ER 
		
		if (numComparisons == 0) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;			
		}
		
		bk.set(numComparisons);
		
		//clean-clean ER (comparisons)		
		for (int e1 : D1entities) {
			for (int e2 : D2entities) {						
				comparison.set(e1+","+e2);
				output.collect(comparison, bk);
				//output.collect(new Text(d1+"###"+d2), new VIntWritable(numComparisons)); //(for ARCS)
			}
		}
		
		
		//dirty ER (comparisons)
		//List<String> prevEntities = new ArrayList<>();
		//for (String entity : entities) {
		//	for (String prevEntity : prevEntities) {
		// ...
	}

}
