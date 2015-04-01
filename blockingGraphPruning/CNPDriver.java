/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphPruning;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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


public class CNPDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphPruning.CNPDriver.class);
		
		conf.setJobName("CNP");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //CNP

		conf.setMapperClass(blockingGraphPruning.NPMapper.class); //common for WNP and CNP
		conf.setReducerClass(blockingGraphPruning.CNP.class); 
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setNumReduceTasks(320);
		
		conf.setCompressMapOutput(true);
		
		//use the following for CNP and CEPTotalOrder
		try{
			Path pt=new Path("/user/hduser/BCin.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Float BCin = Float.parseFloat(br.readLine());
            br.close();            
            conf.setFloat("BCin", BCin);
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    }

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
