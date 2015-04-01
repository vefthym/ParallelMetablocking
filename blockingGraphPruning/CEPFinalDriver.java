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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class CEPFinalDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphPruning.CEPFinalDriver.class);
		
		conf.setJobName("CEP Final");		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //CEP
		
		try{
			Path pt=new Path(args[1]+"/part-00000"); //CEPCounting
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String minValue = br.readLine();
            Integer extraElements = ((Double)Double.parseDouble(br.readLine())).intValue();
            br.close();
            conf.set("min", minValue);
            conf.setInt("extra", extraElements); 
            System.out.println("min="+minValue);
            System.out.println("extra="+extraElements);
            
            if (extraElements > 0) { //use a reducer  to skip the extra elements         	
            	
            	conf.setMapperClass(blockingGraphPruning.CEPFinalMapper.class);
            	conf.setReducerClass(blockingGraphPruning.CEPFinalReducer.class);
    		
            	conf.setNumReduceTasks(56);
            	
            	conf.setMapOutputKeyClass(DoubleWritable.class);
            	conf.setMapOutputValueClass(Text.class);
            } else { //don't use a reducer
            	conf.setMapperClass(blockingGraphPruning.CEPFinalMapperOnly.class);    		
            	conf.setNumReduceTasks(0);
            }
            
            
	    } catch(Exception e){
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
