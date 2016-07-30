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


public class EntityBasedDriverAverageWeightEJS {

	/**
	 *  
	 * @param args should have 3 elements:
	 * args[0]: input blocking collection
	 * args[1]: blocks per entity (file to be stored in distributed cache)	  
	 * args[2]: output of EJS
	 * 
	 * comparisons are stored in HDFS from the NodeDegree job
	 * comparisonsPerEntity are stored in HDFS from the NodeDegree job (after a getmerge and a copyFromLocal) in /user/hduser/nodeDegrees.txt
	 * BCin is stored in HDFS from the EntityIndex job
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(entityBased.EntityBasedDriverAverageWeightEJS.class);

		conf.setJobName("Entity Based Average Weight EJS");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //entity-based output
		

//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); //dirty
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPClean.class); //clean-clean
		conf.setReducerClass(entityBased.EntityBasedReducerAverageWeightEJS.class);

		
		conf.setNumReduceTasks(224);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		
		BufferedReader br = null;		
		try{			
			Path pt=new Path("/user/hduser/comparisons.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Long comparisons = Long.parseLong(br.readLine());
            conf.setLong("comparisons", comparisons);
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close();}
			catch (IOException e) {System.err.println(e.toString());}
	    }		
		
		try {
			DistributedCache.addCacheFile(new URI(args[1]), conf); //blocks per entity
			DistributedCache.addCacheFile(new URI("/user/hduser/nodeDegrees.txt"), conf); //comparisons per entity			
		} catch (URISyntaxException e1) {
			System.err.println(e1.toString());
		}		
		
		client.setConf(conf);
		RunningJob job = null;
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {								
			Counters counters = job.getCounters();
			
			double totalWeight = counters.findCounter(entityBased.EntityBasedReducerAverageWeightEJS.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
			long comparisons = counters.findCounter(entityBased.EntityBasedReducerAverageWeightEJS.Weight.NUM_EDGES).getCounter();
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