package fromExtendedInput;

import java.io.BufferedReader;
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


public class PCNPDriver extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(fromExtendedInput.PCNPDriver.class);
		
		conf.setJobName("PCNP from Extended Input"); //used for PCNP
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
				
		conf.set("weightingScheme", args[0]); //one of: CBS, ECBS, JS, EJS, ARCS
		FileInputFormat.setInputPaths(conf, new Path(args[1])); //ExtendedInput
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); //PCNP

		conf.setMapperClass(fromExtendedInput.PNPMapper.class);		
		conf.setReducerClass(blockingGraphPruning.CNP.class);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
				
		conf.setNumReduceTasks(336);
		
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
		
		conf.setCompressMapOutput(true);

		client.setConf(conf);		
		try {
			JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	

}
