/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import hadoopUtils.MBTools;
import java.io.IOException;
import java.text.DecimalFormat;
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

import advanced.AverageWeightMapperNewFromCompressed.Weight;
import advanced.CEPMapperNew.OutputData;

public class CEPFinalMapperNewEJS extends MapReduceBase implements Mapper<VIntWritable, Text, DoubleWritable, Text> {
  	
	public enum OutputData {PURGED_BLOCKS};
		
	private String weightingScheme;
	double minValue;	
	
	private Text comparison = new Text();
	private DoubleWritable weightToEmit = new DoubleWritable();
	private int cleanBlocks;
	private int dirtyBlocks;
	private long validComparisons;
		
	public void configure (JobConf conf) {		
		weightingScheme = "EJS";	
		minValue = Double.parseDouble(conf.get("min", "0"));		
		cleanBlocks = conf.getInt("cleanBlocks", 0);
		dirtyBlocks = conf.getInt("dirtyBlocks", 0);
		validComparisons = Long.parseLong(conf.get("validComparisons","0"));
	}
	
	public void map(VIntWritable key, Text value,
			OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {	
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
			reporter.progress();
			String[] idsArray = tmpEntityIndex.split(", ");
			entityIds[counter] = Integer.parseInt(idsArray[0].substring(1));
			
			if (entityIds[counter] >= 0) containsPositive = true; else containsNegative = true; //clean-clean (in case this check is NOT made in ExtendeInput Reducer)
			
			int noOfBlocks = idsArray.length-1;
			entityBlocks[counter] = new int[noOfBlocks];
			for (int i=0; i < noOfBlocks; ++i) {
				entityBlocks[counter][i] = Integer.parseInt(idsArray[i+1]);
			}
			counter++;
		}
		
		DecimalFormat df = new DecimalFormat("#.###"); //format doubles to keep only first 4 decimal points (saves space)
		
			
		//dirty ER
		/*int blockId = key.get();
		
		for (int i = 0; i < noOfEntities-1; ++i) {
			reporter.setStatus(++counter+"/"+noOfEntities); 
			for (int j = i+1; j < noOfEntities; ++j) {				
				double weight = MBTools.getWeight(blockId, entityBlocks[i], entityBlocks[j], weightingScheme, dirtyBlocks, validComparisons);
				if (weight >= minValue) { 
					comparison.set(entityIds[i]+","+entityIds[j]);
					weightToEmit.set(Double.parseDouble(df.format(weight)));
					output.collect(weightToEmit, comparison);
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
				if (weight >= minValue) {
					comparison.set(entityIds[i]+","+entityIds[j]);
					weightToEmit.set(Double.parseDouble(df.format(weight)));
					output.collect(weightToEmit, comparison);
				}
			}
		}
	}	
}
