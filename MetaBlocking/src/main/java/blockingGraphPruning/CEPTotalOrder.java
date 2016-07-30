/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.IOException;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;


public class CEPTotalOrder {

	public static void main(String[] args) {		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphPruning.CEPTotalOrder.class);
		
		conf.setJobName("CEPTotalOrder");
		
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //CEPTotalOrder

		conf.setMapperClass(InverseMapper.class);
		conf.setReducerClass(hadoopUtils.InverseReducer.class);		
		
		TotalOrderPartitioner.setPartitionFile(conf, new Path("/user/hduser/_partitions"));
		
		int numReduceTasks = 161;
		
		InputSampler.Sampler<DoubleWritable, Text> sampler =
				new InputSampler.RandomSampler<DoubleWritable, Text>(0.1, numReduceTasks, numReduceTasks - 1);
				
		try {			
			InputSampler.writePartitionFile(conf, sampler);
		} catch (IOException e1) {			
			e1.printStackTrace();
		}

		conf.setPartitionerClass(TotalOrderPartitioner.class);
		
		conf.setNumReduceTasks(numReduceTasks);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
