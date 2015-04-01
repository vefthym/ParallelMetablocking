package blockingGraphBuilding;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class EJSDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.EJSDriver.class);

		conf.setJobName("EJSReducer Driver (intermediate results)");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //AllBlockComparisons
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //EJSReducer Blocking Graph (intermediate)

		conf.setMapperClass(blockingGraphBuilding.EJSMapper.class);		
		conf.setReducerClass(blockingGraphBuilding.EJSReducer.class);
		
		conf.setCompressMapOutput(true);
		
		conf.setNumReduceTasks(160);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.0");
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
