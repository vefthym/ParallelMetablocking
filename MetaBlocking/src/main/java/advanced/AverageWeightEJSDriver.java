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


public class AverageWeightEJSDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.AverageWeightEJSDriver.class);
		
		conf.setJobName("Average Edge Weight using Extended Input"); //used for WEP
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //EJSFinal 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //All unique comparisons with their weight

		conf.setMapperClass(advanced.AverageWeightEJSMapper.class);
		//conf.setCombinerClass(advanced.AverageWeightCombiner.class);
		//conf.setReducerClass(advanced.AverageWeightReducer.class);		
		
		conf.setNumReduceTasks(0);
		
		BufferedReader br = null;
		try{
			Path pt= new Path("/user/hduser/validComparisons.txt");
            FileSystem fs = FileSystem.get(new Configuration());            
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String validComparisons = br.readLine();
            conf.set("validComparisons", validComparisons);           
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); }
			catch (IOException e) {System.err.println(e.toString());}
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
			double totalWeight = counters.findCounter(advanced.AverageWeightEJSMapper.Weight.WEIGHT_COUNTER).getCounter() / 1000.0;			
			long comparisons = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
					"MAP_OUTPUT_RECORDS").getCounter();
			Double averageWeight = (double) totalWeight /  comparisons;
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
