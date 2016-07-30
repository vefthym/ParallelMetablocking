/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class EJSFinalDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EJSFinalDriver.class);
		
		conf.setJobName("Final Preprocessing for EJS");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
		
		conf.setOutputKeyClass(VIntWritable.class); //block id
		conf.setOutputValueClass(Text.class); //list of entities in this block, along with their other blocks and their #comparisons
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //EJS intermediate
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //extended input file (blocking collection with entity index and cardinality)

		conf.setMapperClass(IdentityMapper.class); //forward input to reducer
		conf.setReducerClass(preprocessing.ExtendedInputReducer.class); //just concat the input intarrays, e.g. [][][]...[]
		
		conf.setInt("mapred.task.timeout", 10000000);
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setMaxReduceTaskFailuresPercent(10); //acceptable failures before the whole job fails
		conf.set("mapred.reduce.max.attempts", "10"); //before it is not started again
		conf.set("mapred.max.tracker.failures", "100"); //before it gets black-listed
		conf.set("mapred.job.tracker.handler.count", "40");
		
		conf.setNumReduceTasks(224);

		client.setConf(conf);
		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
