/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;



public class EntityIdsToIntDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EntityIdsToIntDriver.class);

		conf.setJobName("New Ids for entities");

		conf.setOutputKeyClass(VLongWritable.class);
		conf.setOutputValueClass(Text.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //entity collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //entity collection with new int ids

		conf.setMapperClass(preprocessing.EntityIdsToIntMapper.class);		
		//conf.setReducerClass(IdentityReducer.class); 	//just the first time for debugging (counter)
		
		conf.setNumReduceTasks(160); //to merge & sort output in one file				
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
