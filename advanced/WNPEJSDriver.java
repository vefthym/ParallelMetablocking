package advanced;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


public class WNPEJSDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.WNPEJSDriver.class);
		
		conf.setJobName("WNP from Extended Input EJS"); //used for WNP
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //EJSFinal (extended input for EJS)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //WNP

		conf.setMapperClass(advanced.NPMapperEJS.class);		
		conf.setReducerClass(blockingGraphPruning.WNP.class);		
				
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setInt("mapred.task.timeout", 8000000);
		conf.setNumReduceTasks(560);

		BufferedReader br = null;
		try{
			Path pt= new Path("/user/hduser/validComparisons.txt");
            FileSystem fs = FileSystem.get(new Configuration());            
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String validComparisons = br.readLine();
            conf.set("validComparisons", validComparisons);           
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); }
			catch (IOException e) {System.err.println(e.toString());}
	    }
		
		conf.setCompressMapOutput(true);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
