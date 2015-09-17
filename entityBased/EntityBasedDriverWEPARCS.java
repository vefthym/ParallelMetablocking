/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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


public class EntityBasedDriverWEPARCS {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverWEPARCS.class);

		conf.setJobName("Entity Based WEP ARCS");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //entity-based output
		
		//Dirty ER
//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); 
//		conf.setReducerClass(entityBased.EntityBasedReducerWEPARCSDirty.class);
		
		//Clean-Clean ER
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPARCSClean.class); 
		conf.setReducerClass(entityBased.EntityBasedReducerWEPARCSClean.class);
		
		conf.setNumReduceTasks(224);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		
		try{
			Path pt=new Path("/user/hduser/averageWeight.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String weight = br2.readLine();
            br2.close();            
            conf.set("averageWeight", weight); //written from AverageWeight job
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    }
		
		
		client.setConf(conf);
		RunningJob job = null;
		try {
			job = JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}