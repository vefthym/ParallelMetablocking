/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphBuilding;

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
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class BlockingGraph {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.BlockingGraph.class);

		conf.setJobName("Blocking Graph (JS)");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //AllBlockComparisons
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //Blocking Graph weighted

		conf.setMapperClass(IdentityMapper.class);
		//conf.setReducerClass(blockingGraphBuilding.CBS.class);		
//		conf.setReducerClass(blockingGraphBuilding.ECBS.class);
		conf.setReducerClass(blockingGraphBuilding.JS.class);
			//conf.setReducerClass(blockingGraphBuilding.ARCS.class); //use its own Driver
		
		conf.setCombinerClass(blockingGraphBuilding.SumCombiner.class); //works for CBS,JS,ECBS
		
		conf.setCompressMapOutput(true);
		
		conf.setNumReduceTasks(360);
		
		//use the following only for ECBS
		try{
			Path pt=new Path("/user/hduser/numBlocks.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            Long numBlocks = Long.parseLong(br.readLine());
            br.close();
            conf.setLong("numBlocks", numBlocks);
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
