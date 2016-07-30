/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import preprocessing.VIntArrayWritable;


public class EntityBasedDriverCEP1ARCS {

	/**
	 * 
	 * @param args 
	 * args[0] is the input (the blocking collection after block filtering) <br/>
	 * args[1] is the output path <br/>
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverCEP1ARCS.class);

		conf.setJobName("Entity Based CEP (Job 1 ARCS)");
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection		
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //entity-based output


		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); //Dirty
		conf.setReducerClass(entityBased.EntityBasedReducerCEPARCSDirty.class); //Dirty
		
//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPARCSClean.class); //Clean-Clean ER
//		conf.setReducerClass(entityBased.EntityBasedReducerCEPARCSClean.class); //Clean-clean
		
		conf.setNumReduceTasks(728);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setMaxReduceTaskFailuresPercent(10);		
		conf.set("mapred.reduce.max.attempts", "20");
		conf.set("mapred.max.tracker.failures", "200");
		conf.set("mapred.job.tracker.handler.count", "40");
		
		
		client.setConf(conf);
		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}