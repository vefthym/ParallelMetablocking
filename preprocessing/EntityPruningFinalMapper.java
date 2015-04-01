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

public class EntityPruningFinalMapper extends MapReduceBase implements Mapper<VIntWritable, Text, VIntWritable, VIntArrayWritable> {
  
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
		//String []entities = value.toString().split(",");
		List<VIntWritable> D1entities = new ArrayList<>();
		List<VIntWritable> D2entities = new ArrayList<>();
				
		String valueString = value.toString().replaceFirst(";", "");
		StringTokenizer tok = new StringTokenizer(valueString,"#");		
		
		//split the bilateral block in two (clean-clean ER)
		for (Integer entity = Integer.parseInt(tok.nextToken()); tok.hasMoreElements(); entity=Integer.parseInt(tok.nextToken())) {
		//for (String entity : entities) {
			if (entity == null) { continue; }
			if (entity > 0) {
				D1entities.add(new VIntWritable(entity));				
			} else {
				D2entities.add(new VIntWritable(entity));				
			}
			reporter.progress();			
		}
		
		
		if (D1entities.isEmpty() || D2entities.isEmpty()) { //clean-clean ER
			return;
		}
		D1entities.retainAll(nonSingulars);
		D2entities.retainAll(nonSingulars);
		if (D1entities.isEmpty() || D2entities.isEmpty()) { //clean-clean ER. Discards blocks with no comparisons
			return;
		}
		
		D1entities.addAll(D2entities);
		VIntWritable[] toEmitArray = new VIntWritable[D1entities.size()];
		toEmitArray = D1entities.toArray(toEmitArray);
		reporter.progress();
		VIntArrayWritable toEmitFinal = new VIntArrayWritable(toEmitArray);
		
//		StringBuffer outputValue = new StringBuffer();
//		for (VIntWritable e1 : D1entities) {
//			outputValue.append(e1+"#");
//		}
//		outputValue.append(";");
//		for (VIntWritable e2 : D2entities) {
//			outputValue.append(e2+"#");
//		}
		//output.collect(key, new Text(outputValue.toString())); //rewrite the blocks with only nonSingulars
		output.collect(key, toEmitFinal); //rewrite the blocks with only nonSingulars
		
	}

	
}
