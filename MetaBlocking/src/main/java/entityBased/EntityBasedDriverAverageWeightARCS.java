/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
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


public class EntityBasedDriverAverageWeightARCS {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverAverageWeightARCS.class);

		conf.setJobName("Entity Based Average Weight ARCS");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //entity-based output
		
		//Dirty ER
//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); 
//		conf.setReducerClass(entityBased.EntityBasedReducerAverageWeightARCSDirty.class);
		
		//Clean-Clean ER
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPARCSClean.class); 
		conf.setReducerClass(entityBased.EntityBasedReducerAverageWeightARCSClean.class);
		
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
	
			//clean-clean ER
			double totalWeight = counters.findCounter(entityBased.EntityBasedReducerAverageWeightARCSClean.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
			long comparisons = counters.findCounter(entityBased.EntityBasedReducerAverageWeightARCSClean.Weight.NUM_EDGES).getCounter();
			//dirty ER
//			double totalWeight = counters.findCounter(entityBased.EntityBasedReducerAverageWeightARCSDirty.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
//			long comparisons = counters.findCounter(entityBased.EntityBasedReducerAverageWeightARCSDirty.Weight.NUM_EDGES).getCounter();
			
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