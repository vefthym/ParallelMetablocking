/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import hadoopUtils.RelativePositionCompression;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import entityBased.EntityBasedReducerWEP.Output;

import preprocessing.VIntArrayWritable;

public class EntityBasedReducerWEPEJS extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	VIntWritable neighborToEmit = new VIntWritable();
	
	public enum Output {NUM_RECORDS};
	
	private Map<Integer, Double> counters; //key: neighborId, value: #common Blocks
	private Map<Integer, Integer> blocksPerEntity; //key: entityId, value: #blocks containing this entity
	private Map<Integer, Integer> comparisonsPerEntity; //key: entityId, value: #unique comparisons of this entity

	
	private Path[] localFiles;	
	
	long comparisons;
	
	DoubleWritable weightToEmit = new DoubleWritable();
	private double averageWeight;

	public void configure (JobConf conf) {
		counters = new HashMap<>(); 
		blocksPerEntity = new HashMap<>();
		comparisonsPerEntity = new HashMap<>();
		
		comparisons = conf.getLong("comparisons", 0); //default #comparisons is 0
		
		averageWeight = Double.parseDouble(conf.get("averageWeight", "0.0"));
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(conf); 			
			SW = new BufferedReader(new FileReader(localFiles[0].toString()));
			String line;
			while ((line = SW.readLine())!= null) {
				String[] split = line.split("\t");
				blocksPerEntity.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
			}							
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}			
		
		//comparisons per entity
		try {					
			SW = new BufferedReader(new FileReader(localFiles[1].toString()));
			String line;
			while ((line = SW.readLine())!= null) {
				String[] split = line.split("\t");
				comparisonsPerEntity.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
			}							
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
	}

	/**
	 * @param _key an entity id
	 * @param values the list of arrays with entity ids appearing in a block with the _key entity
	 * @param output the input with the values deduplicated (i.e., each entity appearing only once)
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
	OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {		
		int entityId = _key.get();

		counters = new HashMap<>();
		while (values.hasNext()) {
			VIntWritable[] next = RelativePositionCompression.uncompress(values.next()); 
			for (VIntWritable neighborId : next) { 
				if (neighborId.equals(_key)) {
					continue;
				}
				int neighbor = neighborId.get();

				Double count = counters.get(neighbor);
				if (count == null) {
					count = 0.0;
				}				
				counters.put(neighbor, count+1);
			}
		}
		
		double currEntityWeight = Math.log10((double)comparisons/comparisonsPerEntity.get(entityId)); //pre-calculate this only once
		int blocksOfCurrEntity = blocksPerEntity.get(entityId); //pre-calculate this only once
		
		double weight = 0;

		//calculate the weights of the neighbors now
		for (int neighborId : counters.keySet()) {
			weight = 
					(counters.get(neighborId)/(blocksOfCurrEntity+blocksPerEntity.get(neighborId)-counters.get(neighborId))) *
					currEntityWeight *
					Math.log10((double)comparisons/comparisonsPerEntity.get(neighborId));
			if (weight > averageWeight) {
				weightToEmit.set(weight);
				reporter.incrCounter(Output.NUM_RECORDS, 1); //to save space, instead of writing to HDFS
			}
			
		}
				
		
	}

}
