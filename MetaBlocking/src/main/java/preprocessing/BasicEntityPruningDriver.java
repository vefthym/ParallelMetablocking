/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class BasicEntityPruningDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.BasicEntityPruningDriver.class);

		conf.setJobName("Basic Entity Pruning (1rst job)");
		
		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(NullWritable.class);	
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(BZip2Codec.class); //slowest, highest compression ratio
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //block construction without singulars

		conf.setMapperClass(preprocessing.BasicEntityPruningMapper.class);	
		//conf.setCombinerClass(preprocessing.BasicEntityPruningCombiner.class);
		conf.setReducerClass(preprocessing.EntityPruningReducer.class); 	
		//conf.setReducerClass(preprocessing.BasicEntityPruningReducerNew.class);
		
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.0");
		conf.setInt("io.sort.mb", 900); //default 100
		conf.setFloat("io.sort.spill.percent", 0.9f); //default 0.8
//		conf.setFloat("io.sort.record.percent", 0.01f); //default 0.05
		conf.setInt("io.sort.factor", 500); //default 10
		conf.setInt("mapred.task.timeout", 1800000);
		conf.setNumReduceTasks(224); 
		//conf.setNumReduceTasks(0);
				
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
