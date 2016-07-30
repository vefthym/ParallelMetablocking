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

import preprocessing.EJSMapper.OutputData;


public class EJSDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EJSDriver.class);
		
		conf.setJobName("EJS preprocessing from Extended Input");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
	
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntArrayWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //ExtendedInput
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //EJS intermediate

		conf.setMapperClass(preprocessing.EJSMapper.class);		
		conf.setReducerClass(preprocessing.EJSReducer.class);		
		
		//conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
				
		conf.setNumReduceTasks(336);
		
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
		
		try {								
			Counters counters = job.getCounters();			
			Long validComparisons = counters.findCounter(OutputData.VALID_COMPARISONS_X2).getCounter()/2;		
			Path pt=new Path("/user/hduser/validComparisons.txt");			
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
            br.write(validComparisons.toString());            
            br.close();            
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		}
	}
		
	

}
