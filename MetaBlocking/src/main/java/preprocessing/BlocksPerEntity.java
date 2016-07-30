/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import preprocessing.VIntArrayWritable;



class BlocksPerEntityMapper extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, VIntWritable> {
		
	/**
	 * 
	 * @param key an entity id
	 * @param value an array of block ids that this entity belongs to
	 * @param output key: the input key - value: the number of blocks in the input value  
	 */
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, VIntWritable> output, Reporter reporter) throws IOException {				
		output.collect(key, new VIntWritable(value.get().length));
	}

}

public class BlocksPerEntity extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.BlocksPerEntity.class);
		
		conf.setJobName("Number of blocks per entity (from EI)");
		
		conf.setOutputKeyClass(VIntWritable.class); //entity id
		conf.setOutputValueClass(VIntWritable.class); //number of blocks (after block filtering)
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);		

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Entity Index (Filtered with block filtering)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //BlocksPerEntity

		conf.setMapperClass(BlocksPerEntityMapper.class);			
		conf.setNumReduceTasks(0);
		
		client.setConf(conf);
		RunningJob job = null;		
		try {
			job = JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}				
		if (job == null) {
			System.err.println("No job found");
			return;
		}
		
		if (args[1].endsWith("/")) {
			args[1] = args[1].substring(0, args[1].length()-1);
		}
		String mergedFile = args[1]+".txt"; 
		Configuration configuration = new Configuration(); 
		try { 
			FileSystem hdfs = FileSystem.get(configuration); 		
			FileUtil.copyMerge(hdfs, new Path(args[1]), hdfs, new Path(mergedFile), false, configuration, null); 
		} catch (IOException e) { 
			System.err.println(e);
		}
		
	}
		
	

}
