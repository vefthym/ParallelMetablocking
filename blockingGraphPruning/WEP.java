/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WEP extends MapReduceBase implements Mapper<Text, DoubleWritable, Text, DoubleWritable> {
	
	double avgWeight;
	private Path[] localFiles;
	
	public void configure (JobConf job) {
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); 			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); 
			avgWeight = Double.parseDouble(SW.readLine());			
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}	
	}
	
	/**	 
	 * discard all edges with weight lower than the average global weight
	 * @param key i,j entity ids
	 * @param value wij the weight of this edge
	 * @param output identical to intput (identity mapper) for wij > avgWeight
	 */
	public void map(Text key, DoubleWritable value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {		
		if (value.get() > avgWeight) {
			output.collect(key, value); 
		}
	}

}
