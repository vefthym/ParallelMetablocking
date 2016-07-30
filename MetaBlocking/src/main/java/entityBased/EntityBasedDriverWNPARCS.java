/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import preprocessing.VIntArrayWritable;


public class EntityBasedDriverWNPARCS {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverWNPARCS.class);

		conf.setJobName("Entity Based WNP ARCS");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //entity-based output
		
		//Dirty ER
//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); 
//		conf.setReducerClass(entityBased.EntityBasedReducerWNPARCSDirty.class);
		
		//Clean-Clean ER
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPARCSClean.class); 
		conf.setReducerClass(entityBased.EntityBasedReducerWNPARCSClean.class);
		
		conf.setNumReduceTasks(224);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		
		client.setConf(conf);
		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}