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


public class EntityBasedDriverCEP1 {

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
		JobConf conf = new JobConf(entityBased.EntityBasedDriverCEP1.class);

		conf.setJobName("Entity Based CEP (Job 1)");
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
				
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

//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); //Dirty
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPClean.class); //Clean-Clean ER
		conf.setReducerClass(entityBased.EntityBasedReducerCEP.class);
		
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