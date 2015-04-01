package blockingGraphBuilding;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class BlockingGraphARCS {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.BlockingGraphARCS.class);

		conf.setJobName("Blocking Graph (ARCS)");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(VLongWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //ARCS intermediate results
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //Blocking Graph weighted (ARCS)

		conf.setMapperClass(IdentityMapper.class);			
		conf.setReducerClass(blockingGraphBuilding.ARCS.class);
		
		conf.setCompressMapOutput(true);
		
		conf.setNumReduceTasks(360);		

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
