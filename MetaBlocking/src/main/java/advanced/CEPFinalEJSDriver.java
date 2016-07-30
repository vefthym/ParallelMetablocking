/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package advanced;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class CEPFinalEJSDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.CEPFinalDriver.class);
		
		conf.setJobName("CEP Final From Extended Input (EJS)");		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Extended Input
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //CEP
		
		BufferedReader br = null, br2 = null, br3 = null;
		try{
			Path pt=new Path(args[1]+"/part-00000"); //CEPCounting From Extended Input
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String minValue = br.readLine();
            Integer extraElements = ((Double)Double.parseDouble(br.readLine())).intValue();            
            conf.set("min", minValue);
            conf.setInt("extra", extraElements); 
            System.out.println("min="+minValue);
            System.out.println("extra="+extraElements);
            
            if (extraElements > 0) { //use a reducer  to skip the extra elements         	
            	
            	conf.setMapperClass(advanced.CEPFinalMapperNewEJS.class);
            	conf.setReducerClass(blockingGraphPruning.CEPFinalReducer.class); 
    		
            	conf.setNumReduceTasks(56);
            	
            	conf.setMapOutputKeyClass(DoubleWritable.class);
            	conf.setMapOutputValueClass(Text.class);
            } else { //don't use a reducer
            	conf.setMapperClass(advanced.CEPFinalMapperOnlyNewEJS.class);    		
            	conf.setNumReduceTasks(0);
            }
            
            Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            br2=new BufferedReader(new InputStreamReader(fs.open(cleanPath)));
            Integer cleanBlocks = Integer.parseInt(br2.readLine());
            conf.setInt("cleanBlocks", cleanBlocks);
            br3=new BufferedReader(new InputStreamReader(fs.open(dirtyPath)));
            Integer dirtyBlocks = Integer.parseInt(br3.readLine());
            conf.setInt("dirtyBlocks", dirtyBlocks);  
            
           
        	Path pt2= new Path("/user/hduser/validComparisons.txt");                       
        	br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
        	String validComparisons = br2.readLine();
        	conf.set("validComparisons", validComparisons);
           
           
            
	    } catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); br2.close();br3.close(); }
			catch (IOException e) {System.err.println(e.toString());}
	    }
		
		conf.setInt("mapred.task.timeout", 10000000);
//		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setMaxReduceTaskFailuresPercent(10); //acceptable failures before the whole job fails
		conf.set("mapred.reduce.max.attempts", "10"); //before it is not started again
		conf.set("mapred.max.tracker.failures", "100"); //before it gets black-listed
		conf.set("mapred.job.tracker.handler.count", "40");

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
