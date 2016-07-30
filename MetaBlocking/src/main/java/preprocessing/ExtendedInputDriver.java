/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

import preprocessing.ExtendedInputReducer.OutputData;



public class ExtendedInputDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.ExtendedInputDriver.class);
		
		conf.setJobName("Extended Input");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
		
		conf.setOutputKeyClass(VIntWritable.class); //block id
		conf.setOutputValueClass(Text.class); //list of entities in this block, along with their other blocks
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Entity Index
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //extended input file (blocking collection with entity index)

		conf.setMapperClass(preprocessing.ExtendedInputMapper.class);
//		conf.setMapperClass(preprocessing.ExtendedInputMapperARCS.class);
		conf.setReducerClass(preprocessing.ExtendedInputReducer.class);		
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setMaxReduceTaskFailuresPercent(10); //acceptable failures before the whole job fails
		conf.set("mapred.reduce.max.attempts", "10"); //before it is not started again
		conf.set("mapred.max.tracker.failures", "100"); //before it gets black-listed
		conf.set("mapred.job.tracker.handler.count", "40");
		
		conf.setNumReduceTasks(560);
		
		conf.setCompressMapOutput(true);

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
		
		BufferedWriter bwClean = null;
		BufferedWriter bwDirty = null;
		try {								
			Counters counters = job.getCounters();
			Long dirtyBlocks = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
					"REDUCE_OUTPUT_RECORDS").getCounter();
			Long cleanBlocks = counters.findCounter(OutputData.CLEAN_BLOCKS).getCounter();			
			Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            bwClean = new BufferedWriter(new OutputStreamWriter(fs.create(cleanPath,true)));            
            bwDirty = new BufferedWriter(new OutputStreamWriter(fs.create(dirtyPath,true)));
            bwClean.write(cleanBlocks.toString());
            bwDirty.write(dirtyBlocks.toString());            
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		} finally {
			try { bwClean.close(); bwDirty.close();	} 
			catch (IOException e) { System.err.println(e.toString());}			
		}
	}
		
	

}
