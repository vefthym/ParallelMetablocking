/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;


import hadoopUtils.MBTools;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NPMapperNew extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, Text> {
  
	public enum Weight {WEIGHT_COUNTER};
	public enum OutputData {PURGED_BLOCKS};
	public enum InputData {COMPARISONS};
		
	private String weightingScheme;
	
	private int cleanBlocks;
	private int dirtyBlocks;
	
	private VIntWritable ei = new VIntWritable();
	private VIntWritable ej = new VIntWritable();
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
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);

		String[] entityIndices = value.toString().split("]"); //each entityIndex is an array with the first element the entity and the rest elements its blocks
		
		int counter = 0;
		int noOfEntities = entityIndices.length;
		//dirty ER			
//		if (noOfEntities < 2) { //TODO:is it correct ??????
//			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
//			return;
//		}
				
		int[] entityIds = new int[noOfEntities];
		int[][] entityBlocks = new int[noOfEntities][];
		for (String tmpEntityIndex : entityIndices) {
//			if (tmpEntityIndex == null || tmpEntityIndex.length() < 2) {continue;} //GINETAI NA BGALOUME AUTO TON ELEGXO?
			String[] idsArray = tmpEntityIndex.split(", ");
			entityIds[counter] = Integer.parseInt(idsArray[0].substring(1)); //to skip '['
			
			int noOfBlocks = idsArray.length-1;
			entityBlocks[counter] = new int[noOfBlocks];
			for (int i=0; i < noOfBlocks; ++i) {
				entityBlocks[counter][i] = Integer.parseInt(idsArray[i+1]);
			}
			counter++;
		}
		
		DecimalFormat df = new DecimalFormat("#.###"); //format doubles to keep only first 4 decimal points (saves space)
		
		
		//dirty ER
		int blockId = key.get();
		
		reporter.incrCounter(InputData.COMPARISONS, (noOfEntities * (noOfEntities-1))/2);
		
		for (int i = 0; i < noOfEntities-1; ++i) {
			int e1 = entityIds[i];
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {				
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, dirtyBlocks);
				if (weight > 0) {	
					int e2 = entityIds[j];
					Double weightToEmit = Double.parseDouble(df.format(weight));
					ei.set(e1);
					jWij.set(e2+","+weightToEmit);
					output.collect(ei, jWij);
					
					ej.set(e2);
					iWij.set(e1+","+weightToEmit);
					output.collect(ej, iWij);
				}
			}
		}
	}

	
}
