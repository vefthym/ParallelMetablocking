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

public class CEPMapperNew extends MapReduceBase implements Mapper<VIntWritable, Text, DoubleWritable, VIntWritable> {
  	
	public enum OutputData {PURGED_BLOCKS};
		
	private String weightingScheme;	
	private VIntWritable one = new VIntWritable(1);
	private DoubleWritable weightToEmit = new DoubleWritable();
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
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<DoubleWritable, VIntWritable> output, Reporter reporter) throws IOException {	
		reporter.setStatus("splitting the block "+key);		
		
		String[] entityIndices = value.toString().split("]");
		int noOfEntities = entityIndices.length;
		/*//dirty ER			
		if (noOfEntities < 2) { 
			reporter.incrCounter(OutputData.PURGED_BLOCKS, 1);
			return;
		}*/
		
		boolean containsPositive = false; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
		boolean containsNegative = false; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
				
		int counter = 0;		
		int[] entityIds = new int[noOfEntities];
		int[][] entityBlocks = new int[noOfEntities][];
		for (String tmpEntityIndex : entityIndices) {			
			String[] idsArray = tmpEntityIndex.split(", ");
			entityIds[counter] = Integer.parseInt(idsArray[0].substring(1)); //first is entity id
			
			if (entityIds[counter] >= 0) containsPositive = true; else containsNegative = true; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
			
			int noOfBlocks = idsArray.length-1;
			entityBlocks[counter] = new int[noOfBlocks];
			for (int i=0; i < noOfBlocks; ++i) {
				entityBlocks[counter][i] = Integer.parseInt(idsArray[i+1]);
			}
			counter++;
		}
		
		
		//dirty ER
		/*int blockId = key.get();
	
		for (int i = 0; i < noOfEntities-1; ++i) {			
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {				
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, dirtyBlocks, validComparisons);
				if (weight > 0) {				
					weightToEmit.set(weight);
					output.collect(weightToEmit, one);
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
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {				
				if ((entityIds[i] ^ entityIds[j]) >> 31 == 0) { //equal sign bit? (30% faster than if ((e1 > 0 && e2 > 0) || (e1 < 0 && e2 < 0))  ) 
					continue;
				}				
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, cleanBlocks, validComparisons);
				if (weight > 0) {									
					weightToEmit.set(weight);
					output.collect(weightToEmit, one);
				}
			}
		}	
	}
}
