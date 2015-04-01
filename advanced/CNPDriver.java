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


public class CNPDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(advanced.CNPDriver.class);
		
		conf.setJobName("CNP from Extended Input"); //used for CNP
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		conf.set("weightingScheme", args[0]); //one of: CBS, ECBS, JS, ARCS
		FileInputFormat.setInputPaths(conf, new Path(args[1])); //ExtendedInput
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //CNP

		conf.setMapperClass(advanced.NPMapper.class);		
		conf.setReducerClass(blockingGraphPruning.CNP.class);		
				
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setInt("mapred.task.timeout", 10000000);
		
		conf.setNumReduceTasks(448);

		BufferedReader br = null, br2 = null, br3 = null;
		try{
			Path pt=new Path("/user/hduser/BCin.txt");
			Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
			Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Float BCin = Float.parseFloat(br.readLine());
            conf.setFloat("BCin", BCin);
            br2=new BufferedReader(new InputStreamReader(fs.open(cleanPath)));
            Integer cleanBlocks = Integer.parseInt(br2.readLine());
            conf.setInt("cleanBlocks", cleanBlocks);
            br3=new BufferedReader(new InputStreamReader(fs.open(dirtyPath)));
            Integer dirtyBlocks = Integer.parseInt(br3.readLine());
            conf.setInt("dirtyBlocks", dirtyBlocks);            
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    } finally {
	    	try { br.close(); br2.close();br3.close(); }
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
