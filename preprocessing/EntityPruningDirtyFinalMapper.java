/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityPruningDirtyFinalMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
  
	VIntArrayWritable toEmitFinal = new VIntArrayWritable();
		
	private Path[] localFiles;
	private Set<VIntWritable> nonSingulars;
	public void configure(JobConf conf) {
		
		nonSingulars = new HashSet<>();	
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(conf); 			
			SW = new BufferedReader(new FileReader(localFiles[0].toString()));
			String line;
			while ((line = SW.readLine()) != null) {				
				nonSingulars.add(new VIntWritable(Integer.parseInt(line)));				
			}
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
	}
	
	/**
	 * input: a blocking collection
	 * input key: block id
	 * input value: entity ids in this block, separated by ","
	 * output key: entity id (each of the input values)
	 * output value: entity ids separated by " " (neighbors of output key)
	 */	
	public void map(VIntWritable key, Text value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {

		reporter.setStatus("splitting the block "+key);

		List<VIntWritable> entities = new ArrayList<>();
				

		StringTokenizer tok = new StringTokenizer(value.toString(),"#");		
		
		//split the bilateral block in two (clean-clean ER)
		for (Integer entity = Integer.parseInt(tok.nextToken()); tok.hasMoreElements(); entity=Integer.parseInt(tok.nextToken())) {
		//for (String entity : entities) {
			if (entity == null) { continue; }
			entities.add(new VIntWritable(entity));
			reporter.progress();			
		}
		
		
		if (entities.size() < 2) {
			return;
		}
		entities.retainAll(nonSingulars);	//keep only nonSingular entities (discard singular entities)	
		if (entities.size() < 2) { //Discards blocks with no comparisons
			return;
		}
		
		VIntWritable[] toEmitArray = new VIntWritable[entities.size()];
		toEmitArray = entities.toArray(toEmitArray);		
		toEmitFinal.set(toEmitArray);		

		output.collect(key, toEmitFinal); //rewrite the blocks with only nonSingulars
		
	}

	
}
