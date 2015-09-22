/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;


import hadoopUtils.MBTools;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class NPMapperNewFromCompressed extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, Text> {
  
	public enum Weight {WEIGHT_COUNTER};
	public enum OutputData {PURGED_BLOCKS};
	public enum InputData {COMPARISONS};
		
	private String weightingScheme;
	
	private final VIntWritable DELIM = new VIntWritable(Integer.MIN_VALUE);
	
	private int cleanBlocks;
	private int dirtyBlocks;
	
	Text iWij = new Text();
	Text jWij = new Text();
		
	public void configure (JobConf job) {		
		weightingScheme = job.get("weightingScheme"); //one of ARCS,CBS,ECBS,JS,EJS
		cleanBlocks = job.getInt("cleanBlocks", 0);
		dirtyBlocks = job.getInt("dirtyBlocks", 0);
	}
	

	/**
	 * input: an extended blocking collection
	 * @param key block id
	 * @param value arrays of entity ids in this block (first element), along with the block ids (sorted) that contain them (remaining elements)
	 * e.g. [1,7,8,9][3,1,8,10] means that in this block belong the entities 1 and 3 and entity 1 is placed in blocks 7,8,9 (sorted) and 
	 * entity 3 is placed in blocks 1,8,10 
	 * @param output key: entity id (each of the input values). value: entity ids separated by " " (neighbors of output key)
	 */	
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);

		VIntWritable[] inputArray = value.get();
		
		int noOfEntities = 0;
		int lastIndex = 0;
		List<VIntWritable[]> entityIndices = new ArrayList<>();
		int i;
		for	(i = 0; i < inputArray.length; ++i){			
			if (inputArray[i].equals(DELIM)) {
				VIntWritable[] tmpEntityIndex = new VIntWritable[i-lastIndex];
				noOfEntities++;
				System.arraycopy(inputArray, lastIndex, tmpEntityIndex, 0, i-lastIndex);
				entityIndices.add(tmpEntityIndex);
				lastIndex = i+1; //the index of the first element (entity Id) of the new array
			}
		}
		int counter = 0;		
		//dirty ER			
//		if (noOfEntities == 0) { 
//			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
//			return;
//		}
		
		//do the same for the last entity Index
		VIntWritable[] lastEntityIndex = new VIntWritable[i-lastIndex];
		noOfEntities++;
		System.arraycopy(inputArray, lastIndex, lastEntityIndex, 0, i-lastIndex);
		entityIndices.add(lastEntityIndex);
		
				
		VIntWritable[] entityIds = new VIntWritable[noOfEntities];
		int[][] entityBlocks = new int[noOfEntities][];
		for (VIntWritable[] tmpEntityIndex : entityIndices) {
			//if (tmpEntityIndex == null || tmpEntityIndex.length() < 2) {continue;} GINETAI NA BGALOUME AUTO TON ELEGXO?			
			entityIds[counter] = tmpEntityIndex[0];
			
			int noOfBlocks = tmpEntityIndex.length-1;
			entityBlocks[counter] = new int[noOfBlocks];
			for (i=0; i < noOfBlocks; ++i) {
				entityBlocks[counter][i] = tmpEntityIndex[i+1].get();
			}
			counter++;
		}
		
		DecimalFormat df = new DecimalFormat("#.###"); //format doubles to keep only first 4 decimal points (saves space)
		
		//clean-clean ER
		/*List<Integer> D1entities = new ArrayList<>();
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
		}*/
				
		//clean-clean ER
		/*int blockId = key.get();
		List<Integer> blockse1;
		List<Integer> blockse2;
		int counter = 0;
		int D1size = D1entities.size();
		//TODO: add formatting, to skip many decimal digits in weight string
		
		for (int e1 : D1entities) {
			reporter.setStatus(++counter+"/"+D1size);
			blockse1 = entityIndex.get(e1);
			for (int e2 : D2entities) {
				blockse2 = entityIndex.get(e2);
				if (!MBTools.isRepeated(blockse1, blockse2, blockId, weightingScheme)) {
					Double weight = MBTools.getWeight(blockse1, blockse2, blockId, weightingScheme, cleanBlocks, 0);
					Double weightToEmit = Double.parseDouble(df.format(weight));					
					ei.set(e1);
					jWij.set(e2+","+weightToEmit);
					output.collect(ei, jWij);
					
					ej.set(e2);
					iWij.set(e1+","+weightToEmit);
					output.collect(ej, iWij);
				}
			}
		}*/
		
		//dirty ER
		int blockId = key.get();
		
		reporter.incrCounter(InputData.COMPARISONS, (noOfEntities * (noOfEntities-1))/2);		
		for (i = 0; i < noOfEntities-1; ++i) {
			VIntWritable ei = entityIds[i];
			reporter.setStatus(i+1+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {				
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, dirtyBlocks);
				if (weight > 0) {	
					VIntWritable ej = entityIds[j];
					Double weightToEmit = Double.parseDouble(df.format(weight));					
					jWij.set(ej+","+weightToEmit);
					output.collect(ei, jWij);
					
					iWij.set(ei+","+weightToEmit);
					output.collect(ej, iWij);
				}
			}
		}
	}

	
}
