package blockingGraphBuilding;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class ARCSDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.ARCSDriver.class);
		
		conf.setJobName("ARCS intermediate");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VLongWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //input path in HDFS (Entity Index)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //Blocking Graph ARCS (intermediate)

		conf.setMapperClass(blockingGraphBuilding.ARCSMapper.class);
		conf.setReducerClass(blockingGraphBuilding.ARCSReducerDirty.class);
		//conf.setReducerClass(blockingGraphBuilding.ARCSReducer.class);
		
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
