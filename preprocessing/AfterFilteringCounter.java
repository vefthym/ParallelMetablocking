package preprocessing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AfterFilteringCounter {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.AfterFilteringCounter.class);

		conf.setJobName("Block Size Counter from Entity Index");

		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(VIntWritable.class);
				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);		
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //entity index
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //block sizes

		conf.setMapperClass(preprocessing.AfterFilteringBlockSizeCounterMapper.class);
		conf.setCombinerClass(preprocessing.AfterFilteringBlockSizeCounterReducer.class);
		conf.setReducerClass(preprocessing.AfterFilteringBlockSizeCounterReducer.class); 	
		
		conf.setNumReduceTasks(56);				
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
