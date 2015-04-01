package fromExtendedInput;

import hadoopUtils.MBTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CEPFinalEJSMapperOnly extends MapReduceBase implements Mapper<VIntWritable, Text, Text, DoubleWritable> {
  	
	public enum OutputData {PURGED_BLOCKS, OUTPUT_RECORDS};
		
	double minValue;	
	
	private Text comparison = new Text();
	private DoubleWritable weightToEmit = new DoubleWritable();	
	private Long validComparisons;
		
	public void configure (JobConf conf) {				
		minValue = Double.parseDouble(conf.get("min", "0"));		
		validComparisons = Long.parseLong(conf.get("validComparisons"));
	}
	
	public void map(VIntWritable key, Text value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {	
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
			entityIndex.put(entityId, blocks);
		}

		//dirty ER
		/*List<Integer> entities = new ArrayList<>(entityIndex.keySet());				
		if (entities.size() < 2) {
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;
		}*/
		
		
		
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
					if (weight >= minValue) { //edge belongs in top k+extraElements
						comparison.set(e1+","+e2);
						weightToEmit.set(weight);
						//output.collect(comparison, weightToEmit);
						reporter.incrCounter(OutputData.OUTPUT_RECORDS, 1);//to save space
					}
				}
			}
		}
		
		//dirty ER
		/*int blockId = key.get();
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
					if (weight >= minValue) { //edge belongs in top k+extraElements
						comparison.set(e1+","+e2);
						weightToEmit.set(weight);
						//output.collect(comparison, weightToEmit);
						reporter.incrCounter(OutputData.OUTPUT_RECORDS, 1);//to save space
					}
				}
			}
		}*/
		
	}

	
}
