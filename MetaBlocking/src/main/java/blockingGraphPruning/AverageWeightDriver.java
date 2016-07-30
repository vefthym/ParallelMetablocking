/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class AverageWeightDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphPruning.AverageWeightDriver.class);
		
		conf.setJobName("Average Edge Weight"); //used for WEP
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);		

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //Average Edge Weight (one value only)

		conf.setMapperClass(blockingGraphPruning.AverageWeightMapper.class);
		conf.setCombinerClass(blockingGraphPruning.AverageWeightCombiner.class);
		conf.setReducerClass(blockingGraphPruning.AverageWeightReducer.class);		
		
		conf.setNumReduceTasks(1);
		
		conf.setCompressMapOutput(true);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
