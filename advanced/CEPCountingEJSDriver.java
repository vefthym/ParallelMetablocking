/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

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


public class CEPCountingEJSDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.CEPCountingEJSDriver.class);
		
		conf.setJobName("CEP Counting using Extended Input EJS"); //used for CEP
		
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyComparatorClass(hadoopUtils.DescendingDoubleComparator.class); //sort doubles in descending order

				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //EJSFinal
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //minValue and extra (more than k) elements
		
		conf.setMapperClass(advanced.CEPEJSMapper.class);		
		conf.setCombinerClass(blockingGraphPruning.CEPCombiner.class);
		conf.setReducerClass(blockingGraphPruning.CEPReducer.class);		
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setNumReduceTasks(1);
		
		conf.setCompressMapOutput(true);

		BufferedReader br = null, br2 = null;
		try {
			Path pt=new Path("/user/hduser/CEPk.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Integer K = Integer.parseInt(br.readLine());            
            conf.setInt("K", K); 
            System.out.println("K="+K);
            
            Path pt2= new Path("/user/hduser/validComparisons.txt");                       
            br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
            String validComparisons = br2.readLine();
            conf.set("validComparisons", validComparisons);
            
	    } catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); br2.close();}
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
