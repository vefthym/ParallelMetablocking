/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import preprocessing.VIntArrayWritable;


public class EntityBasedDriverAverageWeight {

	/**
	 * 
	 * @param args can be 3 or 4 arguments: <br/>
	 * args[0] is the weighting scheme <br/>
	 * args[1] is the input (the blocking collection after block filtering) <br/>
	 * if the weighting scheme (args[0]) is "CBS" then: <br/>
	 * args[2] is the output path <br/>  
	 * else <br/>
	 * args[2] is the blocks per entity file path and <br/>
	 * args[3] is the output path <br/>
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverAverageWeight.class);

		conf.setJobName("Entity Based Average Weight");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		conf.set("weightingScheme", args[0]);
		FileInputFormat.setInputPaths(conf, new Path(args[1])); //blocking collection
		
		if (args[0].equals("CBS")) {		
			FileOutputFormat.setOutputPath(conf, new Path(args[2])); //entity-based output
		} else {
			try {
				DistributedCache.addCacheFile(new URI(args[2]), conf); //blocks per entity
			} catch (URISyntaxException e1) {
				System.err.println(e1.toString());
			}
			FileOutputFormat.setOutputPath(conf, new Path(args[3])); //entity-based output
		}

		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); //Dirty
//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPClean.class); //Clean-Clean ER
		conf.setReducerClass(entityBased.EntityBasedReducerAverageWeight.class);
		
		conf.setNumReduceTasks(224);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		
		client.setConf(conf);
		RunningJob job = null;
		try {
			job = JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		try {								
			Counters counters = job.getCounters();
			
			double totalWeight = counters.findCounter(entityBased.EntityBasedReducerAverageWeight.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
			long comparisons = counters.findCounter(entityBased.EntityBasedReducerAverageWeight.Weight.NUM_EDGES).getCounter();
			Double averageWeight = totalWeight /  comparisons;
			Path pt=new Path("/user/hduser/averageWeight.txt");
			FileSystem fs = FileSystem.get(new Configuration());
	        BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
	        bw.write(averageWeight.toString());
	        bw.close();
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		}
		
		
		
	}

}