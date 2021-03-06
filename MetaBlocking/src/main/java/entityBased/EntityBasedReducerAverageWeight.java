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
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import preprocessing.VIntArrayWritable;

public class EntityBasedReducerAverageWeight extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
	
	VIntWritable neighborToEmit = new VIntWritable();
	
	public enum Weight {WEIGHT_COUNTER, NUM_EDGES};
	
	private double totalBlocks; //for ECBS	
	private Map<Integer, Double> counters; //key: neighborId, value: #common Blocks
	private Map<Integer, Integer> blocksPerEntity; //key: entityId, value: #blocks containing this entity 

	
	private Path[] localFiles;
	private String weightingScheme = "CBS";
	

	public void configure (JobConf conf) {
		counters = new HashMap<>(); 
		blocksPerEntity = new HashMap<>();				
		
		weightingScheme = conf.get("weightingScheme", "CBS"); //default weighting scheme is CBS
		
		if (!weightingScheme.equals("CBS")) { //nothing more is needed for CBS
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
			if (weightingScheme.equals("ECBS")) { //then we also need #totalBlocks
				try {	
					JobClient client = new JobClient(conf);
				    RunningJob parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id")));
					totalBlocks = parentJob.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
								"MAP_INPUT_RECORDS").getCounter();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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
		
		double sumWeight = 0;

		for (int neighborId : counters.keySet()) {			
			switch (weightingScheme) {
			case "CBS": 
				sumWeight += counters.get(neighborId);//[neighborId]; // CBS
				break;
			case "ECBS":
				sumWeight += counters.get(neighborId)*Math.log10(totalBlocks/blocksPerEntity.get(entityId))*Math.log10(totalBlocks/blocksPerEntity.get(neighborId)); // ECBS
				break;
			case "JS":				
				sumWeight += counters.get(neighborId)/(blocksPerEntity.get(entityId)+blocksPerEntity.get(neighborId)-counters.get(neighborId)); // JS
				break;
			default:
				sumWeight += 0;
			}			
		}
				
		reporter.incrCounter(Weight.WEIGHT_COUNTER, new Double(sumWeight*1000).longValue());
		reporter.incrCounter(Weight.NUM_EDGES, counters.keySet().size());
	}

}
