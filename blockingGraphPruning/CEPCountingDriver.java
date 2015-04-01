package blockingGraphPruning;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
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


public class CEPCountingDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphPruning.CEPCountingDriver.class);
		
		conf.setJobName("CEP Counting (1rst job)");
		
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setOutputKeyComparatorClass(hadoopUtils.DescendingDoubleComparator.class); //sort doubles in descending order
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Blocking Graph 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //minValue and extra (more than k) elements

		conf.setMapperClass(blockingGraphPruning.CEPMapper.class); 
		conf.setCombinerClass(blockingGraphPruning.CEPCombiner.class);
		conf.setReducerClass(blockingGraphPruning.CEPReducer.class); 
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		conf.setNumReduceTasks(1);
		
		conf.setCompressMapOutput(true);

		try{
			Path pt=new Path("/user/hduser/CEPk.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Integer K = Integer.parseInt(br.readLine());
            br.close();
            conf.setInt("K", K); 
            System.out.println("K="+K);
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
