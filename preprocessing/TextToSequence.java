package preprocessing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TextToSequence {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.TextToSequence.class);
		
		conf.setJobName("Text to SequnceFileFormat");

		conf.setOutputKeyClass(VIntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);		
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(preprocessing.TextToSequenceMapper.class);
		conf.setReducerClass(IdentityReducer.class); //used for load balancing
				
		conf.setInt("mapred.task.timeout", 800000);
		
		//conf.setNumReduceTasks(0); //no reducer		
		//conf.setNumReduceTasks(224);
		conf.setNumReduceTasks(112);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
