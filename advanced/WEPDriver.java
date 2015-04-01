package advanced;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


public class WEPDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.WEPDriver.class);
		
		conf.setJobName("WEP from Extended Input"); //used for WEP
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		try{
			Path pt=new Path("/user/hduser/averageWeight.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String weight = br.readLine();
            br.close();            
            conf.set("averageWeight", weight); //written from AverageWeight job
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    }	
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //AverageWeight output 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //WEP

		conf.setMapperClass(advanced.WEPMapperOnly.class);		
	//	conf.setReducerClass(advanced.WEPReducer.class);		
				
		conf.setNumReduceTasks(0);
		
		//conf.setCompressMapOutput(true);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
