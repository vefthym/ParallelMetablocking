/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class PWNPDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.PWNPDriver.class);
		
		conf.setJobName("PWNP from Extended Input"); //used for PWNP
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		conf.set("weightingScheme", args[0]); //one of: CBS, ECBS, JS, EJS, ARCS
		FileInputFormat.setInputPaths(conf, new Path(args[1])); //ExtendedInput
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //PWNP

		conf.setMapperClass(advanced.PNPMapper.class);		
		conf.setReducerClass(blockingGraphPruning.WNP.class);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
				
		conf.setNumReduceTasks(336);
		
		conf.setCompressMapOutput(true);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
