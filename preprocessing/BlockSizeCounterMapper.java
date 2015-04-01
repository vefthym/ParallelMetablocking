package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class BlockSizeCounterMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntWritable> {
			
	public enum InputComparisons {CLEAN_CLEAN, DIRTY};
	VIntWritable inverseUtility = new VIntWritable();	
	
	/**
	 * input key: block id </br>
	 * input value: all entity ids of this block, separated by ',' </br>
	 * output key: cardinality (dirty ER) OR inverseUtility (clean-clean ER) </br>
	 * output value: block id
	 */
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {
				
		String valueString = value.toString().replaceFirst(";", "");
		String []entities = valueString.split("#");
		//VIntWritable[] entities = value.get();
		//if (entities.length < 2) {return;}
		/*inverseUtility.set(entities.length); //dirty ER (||bk|| > ||bl|| <=> |bk| > |bl|)
		
		output.collect(inverseUtility, key); //dirty ER		
*/		//dirty ER stops here. Whatever follows is only for clean-clean ER...
		
		
		//////////////////////////////////////////////////////////////////////
		// STOP HERE FOR DIRTY ER!!!!!
		// START FROM HERE FOR CLEAN-CLEAN ER (after parsing the entities)
		//////////////////////////////////////////////////////////////////////
		int D1counter = 0;
		
		for (String entity : entities) {
			Integer entityId = Integer.parseInt(entity);			
			if (entityId == null) { reporter.setStatus("empty id:"+value); continue; }			
			if (entityId >= 0) {
				D1counter++;
			} 
		}
		
		int D2counter = entities.length - D1counter;
		
		reporter.incrCounter(InputComparisons.CLEAN_CLEAN, (long) D1counter * (long) D2counter);
		reporter.incrCounter(InputComparisons.DIRTY, (entities.length * (entities.length -1) )/2);
		
		
		//instead of taking utility = 1 / max(|D1|, |D2|), 
		//take the inverseUtility = max(|D1|, |D2|) and sort in the reverse order (the smaller the better)
		inverseUtility.set(Math.max(D1counter, D2counter)); //clean-clean ER
		 
		if (D1counter > 0 && D2counter > 0) { //clean-clean ER
			output.collect(inverseUtility, key);
		}
	}
	
	
}
