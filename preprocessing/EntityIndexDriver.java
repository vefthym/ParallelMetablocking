/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.BufferedWriter;
import java.io.IOException;
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

import preprocessing.EntityIndexReducer.OutputData;



public class EntityIndexDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EntityIndexDriver.class);

		conf.setJobName("Entity Index (With Block Filtering)");

		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //entity index

		conf.setMapperClass(preprocessing.EntityIndexMapper.class);		
		conf.setReducerClass(preprocessing.EntityIndexReducer.class);
		//conf.setReducerClass(preprocessing.EntityIndexReducerNoFiltering.class);
		
		conf.setNumReduceTasks(56);
		
		conf.setInt("mapred.task.timeout", 10000000);
		
		try {
			DistributedCache.addCacheFile(new URI(args[1]+"/part-00000"), conf); // block sizes 	
		} catch (URISyntaxException e1) {
			System.err.println(e1.toString());
		}
		
		client.setConf(conf);
		RunningJob job = null;
		try {
			job = JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//the following is used only for CNP,CEPTotalOrder but does not create any overhead (keep it always)		
		if (job == null) {
			System.err.println("No job found");
			return;
		}
		
		try {								
			Counters counters = job.getCounters();
			long entities = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
					"REDUCE_OUTPUT_RECORDS").getCounter();
			long blockAssignments = counters.findCounter(OutputData.BLOCK_ASSIGNMENTS).getCounter();			
			Float BCin = blockAssignments / (float) entities;
			Integer K = ((Double)Math.floor(blockAssignments / 2.0)).intValue();
			Path pt=new Path("/user/hduser/BCin.txt");
			Path k=new Path("/user/hduser/CEPk.txt"); //not tested
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));            
            BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs.create(k,true)));
            br.write(BCin.toString());
            br2.write(K.toString());
            br.close();
            br2.close();
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		}
	}

}