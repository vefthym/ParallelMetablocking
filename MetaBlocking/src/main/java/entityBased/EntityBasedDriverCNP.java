/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import preprocessing.VIntArrayWritable;


public class EntityBasedDriverCNP {

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
		JobConf conf = new JobConf(entityBased.EntityBasedDriverCNP.class);

		conf.setJobName("Entity Based CNP");
		
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

//		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNP.class); //Dirty
		conf.setMapperClass(entityBased.EntityBasedMapperFromCompressedNPClean.class); //Clean-Clean ER
		conf.setReducerClass(entityBased.EntityBasedReducerCNP.class);
		
		conf.setNumReduceTasks(504);
		
		conf.setCompressMapOutput(true);
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.set("io.sort.mb", "400");
		
		
		BufferedReader br = null;
		try{			
			Path pt=new Path("/user/hduser/BCin.txt");
//			Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
//			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Float BCin = Float.parseFloat(br.readLine());
            conf.setFloat("BCin", BCin);
//            br2=new BufferedReader(new InputStreamReader(fs.open(cleanPath)));
//            Integer cleanBlocks = Integer.parseInt(br2.readLine());
//			Path numEntitiesPath=new Path("/user/hduser/numEntities.txt");			
//            FileSystem fs = FileSystem.get(new Configuration());
//            br=new BufferedReader(new InputStreamReader(fs.open(numEntitiesPath)));
//            Integer numEntities = Integer.parseInt(br.readLine());
//            conf.setInt("numEntities", numEntities);            
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); }
			catch (IOException e) {System.err.println(e.toString());}
	    }
		
		
		client.setConf(conf);
		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}