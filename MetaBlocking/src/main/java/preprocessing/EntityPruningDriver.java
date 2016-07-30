/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class EntityPruningDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EntityPruningDriver.class);

		conf.setJobName("Entity Pruning (1rst job)");
		
		conf.setOutputKeyClass(VIntWritable.class);
		//conf.setOutputValueClass(VIntWritable.class);		
		conf.setOutputValueClass(NullWritable.class);	
		//conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntArrayWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setCompressMapOutput(true);
//		conf.setMapOutputCompressorClass(BZip2Codec.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //blocking collection
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //block construction without singulars

		conf.setMapperClass(preprocessing.EntityPruningMapper.class);	
//		conf.setCombinerClass(preprocessing.EntityPruningCombiner.class);
		conf.setReducerClass(preprocessing.EntityPruningReducer.class); 	
		
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.0");
		conf.setInt("io.sort.mb", 700); //default 100
		conf.setFloat("io.sort.spill.percent", 0.9f); //default 0.8
		conf.setInt("io.sort.factor", 100); //default 10
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
