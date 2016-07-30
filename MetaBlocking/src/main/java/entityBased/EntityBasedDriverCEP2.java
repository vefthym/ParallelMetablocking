/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package entityBased;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class EntityBasedDriverCEP2 extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.CEPCountingDriver.class);
		
		conf.setJobName("Entity Based CEP (Job 2)"); 
		
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyComparatorClass(hadoopUtils.DescendingDoubleComparator.class); //sort doubles in descending order
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //CEP1
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //minValue and extra (more than k) elements

		conf.setMapperClass(IdentityMapper.class);		
		conf.setCombinerClass(blockingGraphPruning.CEPCombiner.class);
		conf.setReducerClass(blockingGraphPruning.CEPReducer.class);
		
		conf.setNumReduceTasks(1);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setInt("mapred.task.timeout", 10000000);		
		conf.setMaxReduceTaskFailuresPercent(10);		
		conf.set("mapred.reduce.max.attempts", "10");
		conf.set("mapred.max.tracker.failures", "100");
		conf.set("mapred.job.tracker.handler.count", "40");
		
		conf.setCompressMapOutput(true);

		BufferedReader br = null, br2 = null, br3 = null;
		try {
			Path pt=new Path("/user/hduser/CEPk.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Integer K = Integer.parseInt(br.readLine());
            br.close();
            conf.setInt("K", K); 
            System.out.println("K="+K);
            
            Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            br2=new BufferedReader(new InputStreamReader(fs.open(cleanPath)));
            Integer cleanBlocks = Integer.parseInt(br2.readLine());
            conf.setInt("cleanBlocks", cleanBlocks);
            br3=new BufferedReader(new InputStreamReader(fs.open(dirtyPath)));
            Integer dirtyBlocks = Integer.parseInt(br3.readLine());
            conf.setInt("dirtyBlocks", dirtyBlocks); 
	    } catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); br2.close();br3.close(); }
			catch (IOException e) {System.err.println(e.toString());}
	    }

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
		
	

}
