/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import hadoopUtils.MBTools;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AverageWeightMapperNewFromCompressed extends MapReduceBase implements Mapper<VIntWritable, Text, Text, DoubleWritable> {
  
	public enum Weight {WEIGHT_COUNTER};
	public enum OutputData {PURGED_BLOCKS, COMPARISONS};
		
	private String weightingScheme;
	private Text keyToEmit = new Text();
	private DoubleWritable valueToEmit = new DoubleWritable();
	
	private int cleanBlocks;
	private int dirtyBlocks;
	private long validComparisons;
		
	public void configure (JobConf job) {		
		weightingScheme = job.get("weightingScheme"); //one of ARCS,CBS,ECBS,JS,EJS	
		cleanBlocks = job.getInt("cleanBlocks", 0);
		dirtyBlocks = job.getInt("dirtyBlocks", 0);
		validComparisons = Long.parseLong(job.get("validComparisons","0"));
	}
	

	/**
	 * input: a blocking collection
	 * @param key block id
	 * @param value arrays of entity ids in this block (first element), along with the block ids (sorted) that contain them (remaining elements)
	 * e.g. [1,7,8,9],[3,1,8,10] means that in this block belong the entities 1 and 3 and entity 1 is placed in blocks 7,8,9 (sorted) and 
	 * entity 3 is placed in blocks 1,8,10 
	 * @param output key: entity id (each of the input values). value: entity ids separated by " " (neighbors of output key)
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);
		
		String[] entityIndices = value.toString().split("]"); //each entityIndex is an array with the first element the entity and the rest elements its blocks
		
		int counter = 0;
		int noOfEntities = entityIndices.length;
		//dirty ER			
//		if (noOfEntities < 2) { //TODO:is it correct ??????
//			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
//			return;
//		}
		
		boolean containsPositive = false; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
		boolean containsNegative = false; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
		
		int[] entityIds = new int[noOfEntities]; //the ids of entities contained in this block
		int[][] entityBlocks = new int[noOfEntities][]; //the blocks of each entity
		for (String tmpEntityIndex : entityIndices) {
			//if (tmpEntityIndex == null || tmpEntityIndex.length() < 2) {continue;} GINETAI NA BGALOUME AUTO TON ELEGXO?
			String[] idsArray = tmpEntityIndex.split(", ");
			entityIds[counter] = Integer.parseInt(idsArray[0].substring(1)); //first is entity id
			
			if (entityIds[counter] >= 0) containsPositive = true; else containsNegative = true; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
			
			int noOfBlocks = idsArray.length-1;
			entityBlocks[counter] = new int[noOfBlocks];
			for (int i=0; i < noOfBlocks; ++i) { 
				entityBlocks[counter][i] = Integer.parseInt(idsArray[i+1]); //then are the blocks of this entity
			}
			counter++;
		}

		
		//dirty ER	
		/*int blockId = key.get();		
		for (int i = 0; i < noOfEntities-1; ++i) {
			int e1 = entityIds[i];
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {
				reporter.incrCounter(OutputData.COMPARISONS, 1);
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, dirtyBlocks, validComparisons);
				if (weight > 0) {
					reporter.incrCounter(Weight.WEIGHT_COUNTER, new Double(weight*1000).longValue());				
					keyToEmit.set(e1+","+entityIds[j]);
					valueToEmit.set(weight);
					output.collect(keyToEmit, valueToEmit);
				}
			}
		}*/
		
		//clean-clean ER
		if (!(containsNegative && containsPositive)) { //unless this check is made in ExtendeInput Reducer
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return; //no comparisons from this block
		}
		
		int blockId = key.get();		
		for (int i = 0; i < noOfEntities-1; ++i) {
			int e1 = entityIds[i];
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {
				int e2 = entityIds[j];
				if ((e1 ^ e2) >> 31 == 0) { //equal sign bit? (30% faster than if ((e1 > 0 && e2 > 0) || (e1 < 0 && e2 < 0))  ) 
					continue;
				}
				reporter.incrCounter(OutputData.COMPARISONS, 1);
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, cleanBlocks, validComparisons);
				if (weight > 0) {
					reporter.incrCounter(Weight.WEIGHT_COUNTER, new Double(weight*1000).longValue());				
					keyToEmit.set(e1+","+entityIds[j]);
					valueToEmit.set(weight);
					output.collect(keyToEmit, valueToEmit);
				}
			}
		}
		
	}
	
}

	