/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AfterFilteringByteCounter {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.AfterFilteringByteCounter.class);

		conf.setJobName("Block Size Counter from Entity Index");

		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VLongWritable.class);
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);		
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //entity index
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //block sizes (in bytes)

		conf.setMapperClass(preprocessing.AfterFilteringBlockSizeByteCounterMapper.class);		
		conf.setReducerClass(preprocessing.AfterFilteringBlockSizeByteCounterReducer.class); 
      		conf.setReducerClass(preprocessing.AfterFilteringBlockSizeByteCounterReducerDirty.class); 	

		
		conf.setNumReduceTasks(56);				
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
