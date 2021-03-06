/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class AverageWeightDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.AverageWeightDriver.class);
		
		conf.setJobName("Average Edge Weight using Extended Input"); //used for WEP
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		conf.set("weightingScheme", args[0]); //one of: CBS, ECBS, JS, EJS, ARCS
		FileInputFormat.setInputPaths(conf, new Path(args[1])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //All unique comparisons with their weight

		conf.setMapperClass(advanced.AverageWeightMapperNewFromCompressed.class);
		//conf.setCombinerClass(advanced.AverageWeightCombiner.class);
		//conf.setReducerClass(advanced.AverageWeightReducer.class);		
		
		conf.setNumReduceTasks(0);
		
		BufferedReader br2 = null, br3 = null;
		try{			
			Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br2=new BufferedReader(new InputStreamReader(fs.open(cleanPath)));
            Integer cleanBlocks = Integer.parseInt(br2.readLine());
            conf.setInt("cleanBlocks", cleanBlocks);
            br3=new BufferedReader(new InputStreamReader(fs.open(dirtyPath)));
            Integer dirtyBlocks = Integer.parseInt(br3.readLine());
            conf.setInt("dirtyBlocks", dirtyBlocks);   
            
            if (args[0].equals("EJS")) {
            	Path pt2= new Path("/user/hduser/validComparisons.txt");                       
            	br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
            	String validComparisons = br2.readLine();
            	conf.set("validComparisons", validComparisons);
            }            
            
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br2.close();br3.close(); }
			catch (IOException e) {System.err.println(e.toString());}
	    }
		
		
		
		
//		conf.setCompressMapOutput(true);
		conf.set("mapred.max.tracker.failures", "100"); //before it gets black-listed
		conf.set("mapred.job.tracker.handler.count", "40");
		conf.setInt("mapred.task.timeout", 10000000); //before the non-reporting task fails
		
		
		

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
			double totalWeight = counters.findCounter(advanced.AverageWeightMapperNewFromCompressed.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
			long comparisons = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
					"MAP_OUTPUT_RECORDS").getCounter();
			Double averageWeight = totalWeight /  comparisons;
			Path pt=new Path("/user/hduser/averageWeight.txt");
			FileSystem fs = FileSystem.get(new Configuration());
	        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
	        br.write(averageWeight.toString());
	        br.close();
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		}
	}
		
	

}
