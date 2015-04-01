package advanced;

import hadoopUtils.MBTools;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NPMapperEJS extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, Text> {
  
	public enum OutputData {PURGED_BLOCKS};
	
	private long validComparisons;
	
	private VIntWritable ei = new VIntWritable();
	private VIntWritable ej = new VIntWritable();
	Text iWij = new Text();
	Text jWij = new Text();
		
	public void configure(JobConf job) {
		validComparisons = Long.parseLong(job.get("validComparisons")); //one of ARCS,CBS,ECBS,JS,EJS
	}
	

	/**
	 * input: an extended blocking collection
	 * @param key block id
	 * @param value arrays of entity ids in this block (first element), along with the block ids (sorted) that contain them (remaining elements) and the cardinality (#non-redundatnt comparisons) of this entity
	 * e.g. [1,7,8,9,4][3,1,8,10,10] means that in this block belong the entities 1 and 3 and entity 1 is placed in blocks 7,8,9 (sorted) and 
	 * entity 3 is placed in blocks 1,8,10 and entity 1 participates in 4 comparisons and entity 3 participates in 10 comparisons 
	 * @param output key: entity id (each of the input values). value: entity ids separated by " " (neighbors of output key)
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);		
		
		Map<Integer,List<Integer>> entityIndex = new TreeMap<>(); //key is entity id, value is the list of blocks that contain the key
		List<Integer> blocks;
		String[] entityIndices = value.toString().split("]"); //each entityIndex is an array with the first element the entity and the rest elements its blocks
		for (String tmpEntityIndex : entityIndices) {
			if (tmpEntityIndex == null || tmpEntityIndex.length() < 2) {continue;}
			tmpEntityIndex = tmpEntityIndex.substring(1); //to remove the initial '['
			String[] idsArray = tmpEntityIndex.split(", ");
			int entityId = Integer.parseInt(idsArray[0]);
			blocks = new ArrayList<>(idsArray.length-1); //maybe initial capacity is not needed
			for (int i=1; i < idsArray.length; ++i) {
				blocks.add(Integer.parseInt(idsArray[i]));
			}
			entityIndex.put(entityId, blocks); //the last id in blocks is actually the cardinality of entity
		}

		
		DecimalFormat df = new DecimalFormat("#.###"); //format doubles to keep only first 4 decimal points (saves space)
		
		//dirty ER
		List<Integer> entities = new ArrayList<>(entityIndex.keySet());				
		if (entities.size() < 2) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;
		}
		/*
		//clean-clean ER
		List<Integer> D1entities = new ArrayList<>();
		List<Integer> D2entities = new ArrayList<>();
		for (int entity : entityIndex.keySet()) {
			if (entity < 0) {
				D2entities.add(entity);	
			} else {
				D1entities.add(entity);
			}
		}
		if (D1entities.isEmpty() || D2entities.isEmpty()) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;
		}
		
		
		
		//clean-clean ER
		int blockId = key.get();
		List<Integer> allValuesE1;
		List<Integer> allValuesE2;		
		List<Integer> blockse1;
		List<Integer> blockse2;
		int counter = 0;
		int D1size = D1entities.size();		
		
		for (int e1 : D1entities) {
			reporter.setStatus(++counter+"/"+D1size);
			allValuesE1 = entityIndex.get(e1); //the last value is the cardinality of e1
			blockse1 = allValuesE1.subList(0, allValuesE1.size()-1); //the last value is the cardinality of e1			
			for (int e2 : D2entities) {
				allValuesE2 = entityIndex.get(e2); //the last value is the cardinality of e2
				blockse2 = allValuesE2.subList(0, allValuesE2.size()-1); //the last value is the cardinality of e2
				if (!MBTools.isRepeated(blockse1, blockse2, blockId)) {
					Double weight = MBTools.getWeight(allValuesE1, allValuesE2, blockId, "EJS", 0, validComparisons);
					weight = Double.parseDouble(df.format(weight));
					ei.set(e1);
					jWij.set(e2+","+weight);
					output.collect(ei, jWij);
					
					ej.set(e2);
					iWij.set(e1+","+weight);
					output.collect(ej, iWij);
				}
			}
		}
		*/
		//dirty ER
		int blockId = key.get();
		List<Integer> allValuesE1;
		List<Integer> allValuesE2;		
		List<Integer> blockse1;
		List<Integer> blockse2;		
		int counter = 0;		
		Integer []entitiesArray = new Integer[entities.size()];
		entitiesArray = entities.toArray(entitiesArray);
		int blockSize = entitiesArray.length;
		
		for (int i = 0; i < blockSize-1; ++i) {
			int e1 = entitiesArray[i];
			reporter.setStatus(++counter+"/"+blockSize);
			allValuesE1 = entityIndex.get(e1); //the last value is the cardinality of e1
			blockse1 = allValuesE1.subList(0, allValuesE1.size()-1); //the last value is the cardinality of e1
			for (int j = i+1; j < blockSize; ++j) {
				int e2 = entitiesArray[j];
				allValuesE2 = entityIndex.get(e2); //the last value is the cardinality of e2
				blockse2 = allValuesE2.subList(0, allValuesE2.size()-1); //the last value is the cardinality of e2
				
				if (!MBTools.isRepeated(blockse1, blockse2, blockId)) {
					Double weight = MBTools.getWeight(allValuesE1, allValuesE2, blockId, "EJS", 0, validComparisons);
					weight = Double.parseDouble(df.format(weight));
					ei.set(e1);
					jWij.set(e2+","+weight);
					output.collect(ei, jWij);
					
					ej.set(e2);
					iWij.set(e1+","+weight);
					output.collect(ej, iWij);
				}
			}
		}
	}

	
}
