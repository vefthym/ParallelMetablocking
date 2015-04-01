package blockingGraphBuilding;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.collections.comparators.ReverseComparator;
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
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


import blockingGraphBuilding.AllBlockComparisonsReducer.OutputData;


public class AllBlockComparisonsDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.AllBlockComparisonsDriver.class);
		
		conf.setJobName("AllBlockComparisons");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //input path in HDFS (Entity Index)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //output path in HDFS (blocks)

		conf.setMapperClass(blockingGraphBuilding.AllBlockComparisonsMapper.class);
		conf.setReducerClass(blockingGraphBuilding.AllBlockComparisonsReducer.class);
		//conf.setReducerClass(blockingGraphBuilding.AllBlockComparisonsReducerDirty.class);

		
		conf.setNumReduceTasks(360);

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
		
		//the following is used only for ECBS but does not create any overhead (keep it always)
		try {								
			Counters counters = job.getCounters();
			long ReduceInputGroups = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
					"REDUCE_INPUT_GROUPS").getCounter();
			long purgedBlocks = counters.findCounter(OutputData.PURGED_BLOCKS).getCounter();			
			Long numBlocks = ReduceInputGroups - purgedBlocks;
			Path pt=new Path("/user/hduser/numBlocks.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));            
            br.write(numBlocks.toString());
            br.close();
		} catch (IllegalArgumentException | IOException e) {			
			System.err.println(e.toString());
		}
	}
		
	

}
