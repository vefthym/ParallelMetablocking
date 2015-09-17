/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;

public class EntityBasedIndexReducerMemory extends MapReduceBase implements Reducer<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> {
	
	
	static enum OutputData {D1Entities, D2Entities, BLOCK_ASSIGNMENTS, CACHE_HITS}; 
	
	Map<Integer, List<VIntWritable>> blockCache;
	Set<VIntWritable> entities; //the entities that belong to a common block with _key entity
	
	public void configure(JobConf conf) {
		blockCache = new HashMap<>(); //caches the block contents of each reducer (local)
		//do a single scan in the input blocking collection to load it in memory
		BufferedReader br=null;
		try{
//					FileSystem fs = FileSystem.get(new Configuration());
//					Path inFile = new Path("/user/hduser/dbpediaDirtyRaw.txt");
//					br = new BufferedReader(new InputStreamReader(fs.open(inFile))); //OPTION 1: read from HDFS
			br = new BufferedReader(new FileReader("/home/user/dbpediaDirtyRaw.txt")); //OPTION 2: read from local FS
			String line;
			while ((line = br.readLine()) != null) {	
				List<VIntWritable> blockEntitiesList = new ArrayList<>(); //the entities of this block
				String block[] = line.split("\t"); //first part is id, second part is contents (entity Ids)
				int blockId = Integer.parseInt(block[0]);									
				String[] blockEntities = block[1].split("#");
				for (String eId : blockEntities) {
					if (eId != "") {							
						blockEntitiesList.add(new VIntWritable(Integer.parseInt(eId)));
					}
				}
				blockCache.put(blockId, blockEntitiesList);
			}
		}catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close();}
			catch (IOException e) {System.err.println(e.toString());}
	    }
	}
	
	/**
	 * 
	 * @param _key entity id
	 * @param values block ids of the current entity 
	 */
	public void reduce(VIntWritable _key, Iterator<VIntWritable> values,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {

		Set<Integer> blocks = new HashSet<>(); //the blocks of the _key entity
		entities = new TreeSet<>();
		
		while (values.hasNext()) {
			Integer block = values.next().get(); //the block id
			blocks.add(block);
		}		
		
		for (Integer block : blocks) {
			entities.addAll(blockCache.get(block));
		}
		
		entities.remove(_key);
		reporter.setStatus("Writing blocks");
		
		VIntWritable[] tmpArray = new VIntWritable[entities.size()];
		tmpArray = entities.toArray(tmpArray);
		VIntArrayWritable toEmit = new VIntArrayWritable(tmpArray);
		
		output.collect(_key, toEmit);
		
		
	}
	
		
}
